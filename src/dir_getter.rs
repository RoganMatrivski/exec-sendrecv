use std::path::PathBuf;

use directories::ProjectDirs;

pub fn get_data_dir(install: bool) -> eyre::Result<Option<PathBuf>> {
    let Some(dir) = ProjectDirs::from("id.my", "rgmtrv", "exec_sendrecv") else {
        return Ok(None);
    };

    let data_projdir = dir.data_dir();

    if install {
        std::fs::create_dir_all(data_projdir)?;
    }

    let path = if data_projdir.try_exists()? {
        data_projdir
    } else {
        std::fs::create_dir_all(dir.cache_dir())?;
        dir.cache_dir()
    };

    Ok(Some(path.to_path_buf()))
}
