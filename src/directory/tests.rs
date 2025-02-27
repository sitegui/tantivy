use super::*;
use std::io::Write;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time;
use std::time::Duration;

#[test]
fn test_ram_directory() {
    let mut ram_directory = RAMDirectory::create();
    test_directory(&mut ram_directory);
}

#[test]
#[cfg(feature = "mmap")]
fn test_mmap_directory() {
    let mut mmap_directory = MmapDirectory::create_from_tempdir().unwrap();
    test_directory(&mut mmap_directory);
}

#[test]
#[should_panic]
fn ram_directory_panics_if_flush_forgotten() {
    let test_path: &'static Path = Path::new("some_path_for_test");
    let mut ram_directory = RAMDirectory::create();
    let mut write_file = ram_directory.open_write(test_path).unwrap();
    assert!(write_file.write_all(&[4]).is_ok());
}

fn test_simple(directory: &mut dyn Directory) {
    let test_path: &'static Path = Path::new("some_path_for_test");
    {
        let mut write_file = directory.open_write(test_path).unwrap();
        assert!(directory.exists(test_path));
        write_file.write_all(&[4]).unwrap();
        write_file.write_all(&[3]).unwrap();
        write_file.write_all(&[7, 3, 5]).unwrap();
        write_file.flush().unwrap();
    }
    {
        let read_file = directory.open_read(test_path).unwrap();
        let data: &[u8] = &*read_file;
        assert_eq!(data, &[4u8, 3u8, 7u8, 3u8, 5u8]);
    }
    assert!(directory.delete(test_path).is_ok());
    assert!(!directory.exists(test_path));
}

fn test_rewrite_forbidden(directory: &mut dyn Directory) {
    let test_path: &'static Path = Path::new("some_path_for_test");
    {
        directory.open_write(test_path).unwrap();
        assert!(directory.exists(test_path));
    }
    {
        assert!(directory.open_write(test_path).is_err());
    }
    assert!(directory.delete(test_path).is_ok());
}

fn test_write_create_the_file(directory: &mut dyn Directory) {
    let test_path: &'static Path = Path::new("some_path_for_test");
    {
        assert!(directory.open_read(test_path).is_err());
        let _w = directory.open_write(test_path).unwrap();
        assert!(directory.exists(test_path));
        assert!(directory.open_read(test_path).is_ok());
        assert!(directory.delete(test_path).is_ok());
    }
}

fn test_directory_delete(directory: &mut dyn Directory) {
    let test_path: &'static Path = Path::new("some_path_for_test");
    assert!(directory.open_read(test_path).is_err());
    let mut write_file = directory.open_write(&test_path).unwrap();
    write_file.write_all(&[1, 2, 3, 4]).unwrap();
    write_file.flush().unwrap();
    {
        let read_handle = directory.open_read(&test_path).unwrap();
        assert_eq!(&*read_handle, &[1u8, 2u8, 3u8, 4u8]);
        // Mapped files can't be deleted on Windows
        if !cfg!(windows) {
            assert!(directory.delete(&test_path).is_ok());
            assert_eq!(&*read_handle, &[1u8, 2u8, 3u8, 4u8]);
        }

        assert!(directory.delete(Path::new("SomeOtherPath")).is_err());
    }

    if cfg!(windows) {
        assert!(directory.delete(&test_path).is_ok());
    }

    assert!(directory.open_read(&test_path).is_err());
    assert!(directory.delete(&test_path).is_err());
}

fn test_directory(directory: &mut dyn Directory) {
    test_simple(directory);
    test_rewrite_forbidden(directory);
    test_write_create_the_file(directory);
    test_directory_delete(directory);
    test_lock_non_blocking(directory);
    test_lock_blocking(directory);
    test_watch(directory);
}

fn test_watch(directory: &mut dyn Directory) {
    let counter: Arc<AtomicUsize> = Default::default();
    let counter_clone = counter.clone();
    let watch_callback = Box::new(move || {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    });
    assert!(directory
        .atomic_write(Path::new("meta.json"), b"random_test_data")
        .is_ok());
    thread::sleep(Duration::new(0, 10_000));
    assert_eq!(0, counter.load(Ordering::SeqCst));

    let watch_handle = directory.watch(watch_callback).unwrap();
    for i in 0..10 {
        assert_eq!(i, counter.load(Ordering::SeqCst));
        assert!(directory
            .atomic_write(Path::new("meta.json"), b"random_test_data_2")
            .is_ok());
        for _ in 0..1_000 {
            if counter.load(Ordering::SeqCst) > i {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(i + 1, counter.load(Ordering::SeqCst));
    }
    mem::drop(watch_handle);
    assert!(directory
        .atomic_write(Path::new("meta.json"), b"random_test_data")
        .is_ok());
    thread::sleep(Duration::from_millis(200));
    assert_eq!(10, counter.load(Ordering::SeqCst));
}

fn test_lock_non_blocking(directory: &mut dyn Directory) {
    {
        let lock_a_res = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: false,
        });
        assert!(lock_a_res.is_ok());
        let lock_b_res = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("b.lock"),
            is_blocking: false,
        });
        assert!(lock_b_res.is_ok());
        let lock_a_res2 = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: false,
        });
        assert!(lock_a_res2.is_err());
    }
    let lock_a_res = directory.acquire_lock(&Lock {
        filepath: PathBuf::from("a.lock"),
        is_blocking: false,
    });
    assert!(lock_a_res.is_ok());
}

fn test_lock_blocking(directory: &mut dyn Directory) {
    let lock_a_res = directory.acquire_lock(&Lock {
        filepath: PathBuf::from("a.lock"),
        is_blocking: true,
    });
    assert!(lock_a_res.is_ok());
    std::thread::spawn(move || {
        //< lock_a_res is sent to the thread.
        std::thread::sleep(time::Duration::from_millis(10));
        // explicitely droping lock_a_res. It would have been sufficient to just force it
        // to be part of the move, but the intent seems clearer that way.
        drop(lock_a_res);
    });
    {
        // A non-blocking call should fail, as the thread is running and holding the lock.
        let lock_a_res = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: false,
        });
        assert!(lock_a_res.is_err());
    }
    {
        // the blocking call should wait for at least 10ms.
        let start = time::Instant::now();
        let lock_a_res = directory.acquire_lock(&Lock {
            filepath: PathBuf::from("a.lock"),
            is_blocking: true,
        });
        assert!(lock_a_res.is_ok());
        assert!(start.elapsed().subsec_millis() >= 10);
    }
}
