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
