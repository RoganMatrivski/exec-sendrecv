fn kill_tree(child: &mut std::process::Child) {
    use sysinfo::{Pid, System};
    use tracing::{error, warn};

    let sys = System::new_all();
    let pid = Pid::from_u32(child.id());

    let Some(process) = sys.process(pid) else {
        warn!("process {} not found", pid);
        return;
    };

    match process.kill_and_wait() {
        Ok(Some(status)) => {
            tracing::debug!(
                "process {} killed successfully with status {:?}",
                pid,
                status
            );
        }
        Ok(None) => {
            warn!("process {} killed but no exit status available", pid);
        }
        Err(err) => {
            error!("failed to kill process {}: {:?}", pid, err);
        }
    }
}
