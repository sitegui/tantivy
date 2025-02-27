use super::operation::AddOperation;
use crate::core::Segment;
use crate::core::SerializableSegment;
use crate::fastfield::FastFieldsWriter;
use crate::fieldnorm::FieldNormsWriter;
use crate::indexer::segment_serializer::SegmentSerializer;
use crate::postings::compute_table_size;
use crate::postings::MultiFieldPostingsWriter;
use crate::schema::FieldEntry;
use crate::schema::FieldType;
use crate::schema::Schema;
use crate::schema::Term;
use crate::schema::Value;
use crate::tokenizer::BoxedTokenizer;
use crate::tokenizer::FacetTokenizer;
use crate::tokenizer::{TokenStream, Tokenizer};
use crate::DocId;
use crate::Opstamp;
use crate::Result;
use crate::TantivyError;
use std::io;
use std::str;

/// Computes the initial size of the hash table.
///
/// Returns a number of bit `b`, such that the recommended initial table size is 2^b.
fn initial_table_size(per_thread_memory_budget: usize) -> Result<usize> {
    let table_memory_upper_bound = per_thread_memory_budget / 3;
    if let Some(limit) = (10..)
        .take_while(|num_bits: &usize| compute_table_size(*num_bits) < table_memory_upper_bound)
        .last()
    {
        Ok(limit.min(19)) // we cap it at 2^19 = 512K.
    } else {
        Err(TantivyError::InvalidArgument(
            format!("per thread memory budget (={}) is too small. Raise the memory budget or lower the number of threads.", per_thread_memory_budget)))
    }
}

/// A `SegmentWriter` is in charge of creating segment index from a
/// set of documents.
///
/// They creates the postings list in anonymous memory.
/// The segment is layed on disk when the segment gets `finalized`.
pub struct SegmentWriter {
    max_doc: DocId,
    multifield_postings: MultiFieldPostingsWriter,
    segment_serializer: SegmentSerializer,
    fast_field_writers: FastFieldsWriter,
    fieldnorms_writer: FieldNormsWriter,
    doc_opstamps: Vec<Opstamp>,
    tokenizers: Vec<Option<BoxedTokenizer>>,
}

impl SegmentWriter {
    /// Creates a new `SegmentWriter`
    ///
    /// The arguments are defined as follows
    ///
    /// - heap: most of the segment writer data (terms, and postings lists recorders)
    /// is stored in a user-defined heap object. This makes it possible for the user to define
    /// the flushing behavior as a buffer limit
    /// - segment: The segment being written
    /// - schema
    pub fn for_segment(
        memory_budget: usize,
        mut segment: Segment,
        schema: &Schema,
    ) -> Result<SegmentWriter> {
        let table_num_bits = initial_table_size(memory_budget)?;
        let segment_serializer = SegmentSerializer::for_segment(&mut segment)?;
        let multifield_postings = MultiFieldPostingsWriter::new(schema, table_num_bits);
        let tokenizers =
            schema
                .fields()
                .iter()
                .map(FieldEntry::field_type)
                .map(|field_type| match *field_type {
                    FieldType::Str(ref text_options) => text_options
                        .get_indexing_options()
                        .and_then(|text_index_option| {
                            let tokenizer_name = &text_index_option.tokenizer();
                            segment.index().tokenizers().get(tokenizer_name)
                        }),
                    _ => None,
                })
                .collect();
        Ok(SegmentWriter {
            max_doc: 0,
            multifield_postings,
            fieldnorms_writer: FieldNormsWriter::for_schema(schema),
            segment_serializer,
            fast_field_writers: FastFieldsWriter::from_schema(schema),
            doc_opstamps: Vec::with_capacity(1_000),
            tokenizers,
        })
    }

    /// Lay on disk the current content of the `SegmentWriter`
    ///
    /// Finalize consumes the `SegmentWriter`, so that it cannot
    /// be used afterwards.
    pub fn finalize(mut self) -> Result<Vec<u64>> {
        self.fieldnorms_writer.fill_up_to_max_doc(self.max_doc);
        write(
            &self.multifield_postings,
            &self.fast_field_writers,
            &self.fieldnorms_writer,
            self.segment_serializer,
        )?;
        Ok(self.doc_opstamps)
    }

    pub fn mem_usage(&self) -> usize {
        self.multifield_postings.mem_usage()
    }

    /// Indexes a new document
    ///
    /// As a user, you should rather use `IndexWriter`'s add_document.
    pub fn add_document(&mut self, add_operation: AddOperation, schema: &Schema) -> io::Result<()> {
        let doc_id = self.max_doc;
        let mut doc = add_operation.document;
        self.doc_opstamps.push(add_operation.opstamp);

        self.fast_field_writers.add_document(&doc);

        for (field, field_values) in doc.get_sorted_field_values() {
            let field_options = schema.get_field_entry(field);
            if !field_options.is_indexed() {
                continue;
            }
            match *field_options.field_type() {
                FieldType::HierarchicalFacet => {
                    let facets: Vec<&str> = field_values
                        .iter()
                        .flat_map(|field_value| match *field_value.value() {
                            Value::Facet(ref facet) => Some(facet.encoded_str()),
                            _ => {
                                panic!("Expected hierarchical facet");
                            }
                        })
                        .collect();
                    let mut term = Term::for_field(field); // we set the Term
                    for fake_str in facets {
                        let mut unordered_term_id_opt = None;
                        FacetTokenizer.token_stream(fake_str).process(&mut |token| {
                            term.set_text(&token.text);
                            let unordered_term_id =
                                self.multifield_postings.subscribe(doc_id, &term);
                            unordered_term_id_opt = Some(unordered_term_id);
                        });
                        if let Some(unordered_term_id) = unordered_term_id_opt {
                            self.fast_field_writers
                                .get_multivalue_writer(field)
                                .expect("multified writer for facet missing")
                                .add_val(unordered_term_id);
                        }
                    }
                }
                FieldType::Str(_) => {
                    let num_tokens = if let Some(ref mut tokenizer) =
                        self.tokenizers[field.0 as usize]
                    {
                        let texts: Vec<&str> = field_values
                            .iter()
                            .flat_map(|field_value| match *field_value.value() {
                                Value::Str(ref text) => Some(text.as_str()),
                                _ => None,
                            })
                            .collect();
                        if texts.is_empty() {
                            0
                        } else {
                            let mut token_stream = tokenizer.token_stream_texts(&texts[..]);
                            self.multifield_postings
                                .index_text(doc_id, field, &mut token_stream)
                        }
                    } else {
                        0
                    };
                    self.fieldnorms_writer.record(doc_id, field, num_tokens);
                }
                FieldType::U64(ref int_option) => {
                    if int_option.is_indexed() {
                        for field_value in field_values {
                            let term = Term::from_field_u64(
                                field_value.field(),
                                field_value.value().u64_value(),
                            );
                            self.multifield_postings.subscribe(doc_id, &term);
                        }
                    }
                }
                FieldType::Date(ref int_option) => {
                    if int_option.is_indexed() {
                        for field_value in field_values {
                            let term = Term::from_field_i64(
                                field_value.field(),
                                field_value.value().date_value().timestamp(),
                            );
                            self.multifield_postings.subscribe(doc_id, &term);
                        }
                    }
                }
                FieldType::I64(ref int_option) => {
                    if int_option.is_indexed() {
                        for field_value in field_values {
                            let term = Term::from_field_i64(
                                field_value.field(),
                                field_value.value().i64_value(),
                            );
                            self.multifield_postings.subscribe(doc_id, &term);
                        }
                    }
                }
                FieldType::F64(ref int_option) => {
                    if int_option.is_indexed() {
                        for field_value in field_values {
                            let term = Term::from_field_f64(
                                field_value.field(),
                                field_value.value().f64_value(),
                            );
                            self.multifield_postings.subscribe(doc_id, &term);
                        }
                    }
                }
                FieldType::Bytes => {
                    // Do nothing. Bytes only supports fast fields.
                }
            }
        }
        doc.filter_fields(|field| schema.get_field_entry(field).is_stored());
        let doc_writer = self.segment_serializer.get_store_writer();
        doc_writer.store(&doc)?;
        self.max_doc += 1;
        Ok(())
    }

    /// Max doc is
    /// - the number of documents in the segment assuming there is no deletes
    /// - the maximum document id (including deleted documents) + 1
    ///
    /// Currently, **tantivy** does not handle deletes anyway,
    /// so `max_doc == num_docs`
    pub fn max_doc(&self) -> u32 {
        self.max_doc
    }

    /// Number of documents in the index.
    /// Deleted documents are not counted.
    ///
    /// Currently, **tantivy** does not handle deletes anyway,
    /// so `max_doc == num_docs`
    #[allow(dead_code)]
    pub fn num_docs(&self) -> u32 {
        self.max_doc
    }
}

// This method is used as a trick to workaround the borrow checker
fn write(
    multifield_postings: &MultiFieldPostingsWriter,
    fast_field_writers: &FastFieldsWriter,
    fieldnorms_writer: &FieldNormsWriter,
    mut serializer: SegmentSerializer,
) -> Result<()> {
    let term_ord_map = multifield_postings.serialize(serializer.get_postings_serializer())?;
    fast_field_writers.serialize(serializer.get_fast_field_serializer(), &term_ord_map)?;
    fieldnorms_writer.serialize(serializer.get_fieldnorms_serializer())?;
    serializer.close()?;
    Ok(())
}

impl SerializableSegment for SegmentWriter {
    fn write(&self, serializer: SegmentSerializer) -> Result<u32> {
        let max_doc = self.max_doc;
        write(
            &self.multifield_postings,
            &self.fast_field_writers,
            &self.fieldnorms_writer,
            serializer,
        )?;
        Ok(max_doc)
    }
}

#[cfg(test)]
mod tests {
    use super::initial_table_size;

    #[test]
    fn test_hashmap_size() {
        assert_eq!(initial_table_size(100_000).unwrap(), 11);
        assert_eq!(initial_table_size(1_000_000).unwrap(), 14);
        assert_eq!(initial_table_size(10_000_000).unwrap(), 17);
        assert_eq!(initial_table_size(1_000_000_000).unwrap(), 19);
    }
}
