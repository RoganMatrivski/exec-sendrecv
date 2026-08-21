pub fn get_executable<T: AsRef<std::path::Path>>(path: T) -> Option<std::path::PathBuf> {
    // Fn body courtesy of Gemini
    use is_executable::IsExecutable;
    use std::collections::VecDeque;

    let root = path.as_ref();
    if root.is_file() && root.is_executable() {
        return Some(root.to_path_buf());
    }

    let mut queue = VecDeque::new();
    queue.push_back(root.to_path_buf());

    while let Some(current_dir) = queue.pop_front() {
        let Ok(entries) = std::fs::read_dir(&current_dir) else {
            continue;
        };

        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_file() && entry_path.is_executable() {
                return Some(entry_path);
            } else if entry_path.is_dir() {
                queue.push_back(entry_path);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_executable(path: &std::path::Path) {
        std::fs::write(path, b"dummy content").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(path, perms).unwrap();
        }
    }

    fn exe_name(name: &str) -> String {
        #[cfg(windows)]
        {
            format!("{name}.exe")
        }
        #[cfg(not(windows))]
        {
            name.to_string()
        }
    }

    #[test]
    fn test_non_existent_path() {
        let dir = tempdir().unwrap();
        let non_existent = dir.path().join("does_not_exist");
        assert_eq!(get_executable(non_existent), None);
    }

    #[test]
    fn test_direct_file_non_executable() {
        let dir = tempdir().unwrap();
        let text_file = dir.path().join("readme.txt");
        std::fs::write(&text_file, b"hello").unwrap();

        assert_eq!(get_executable(&text_file), None);
    }

    #[test]
    fn test_direct_file_executable() {
        let dir = tempdir().unwrap();
        let exe_path = dir.path().join(exe_name("my_app"));
        make_executable(&exe_path);

        let result = get_executable(&exe_path);
        assert_eq!(result, Some(exe_path));
    }

    #[test]
    fn test_empty_directory() {
        let dir = tempdir().unwrap();
        assert_eq!(get_executable(dir.path()), None);
    }

    #[test]
    fn test_directory_with_only_non_executables() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"some text").unwrap();
        std::fs::write(dir.path().join("config.json"), b"{}").unwrap();
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub").join("data.csv"), b"1,2,3").unwrap();

        assert_eq!(get_executable(dir.path()), None);
    }

    #[test]
    fn test_directory_with_executable_in_root() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"some text").unwrap();
        let exe_path = dir.path().join(exe_name("runner"));
        make_executable(&exe_path);

        assert_eq!(get_executable(dir.path()), Some(exe_path));
    }

    #[test]
    fn test_directory_with_nested_executable() {
        let dir = tempdir().unwrap();
        let nested_dir = dir.path().join("nested").join("deep");
        std::fs::create_dir_all(&nested_dir).unwrap();
        let exe_path = nested_dir.join(exe_name("nested_app"));
        make_executable(&exe_path);

        assert_eq!(get_executable(dir.path()), Some(exe_path));
    }

    #[test]
    fn test_bfs_order_finds_shallower_executable_first() {
        let dir = tempdir().unwrap();
        let sub_dir = dir.path().join("subdir");
        std::fs::create_dir_all(&sub_dir).unwrap();

        // Create a deep executable in subdir
        let deep_exe = sub_dir.join(exe_name("deep_app"));
        make_executable(&deep_exe);

        // Create a shallow executable at root
        let shallow_exe = dir.path().join(exe_name("shallow_app"));
        make_executable(&shallow_exe);

        // BFS must encounter shallow_exe at depth 1 before exploring subdir at depth 2
        let result = get_executable(dir.path());
        assert_eq!(result, Some(shallow_exe));
    }
}

