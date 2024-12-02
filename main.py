import os
import concurrent.futures
import logging
import paramiko
from datetime import datetime
from typing import List, Dict, Optional
from functools import wraps
import time
import socket

# Configurable Constants
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
CONNECTION_TIMEOUT = 30  # seconds

# Enhanced Logging Setup
def setup_logging(log_file: str) -> logging.Logger:
    """Configure comprehensive logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# Robust Retry Decorator
def retry_on_network_error(max_retries: int = MAX_RETRIES):
    """Decorator to retry function on network-related errors"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (paramiko.SSHException, socket.error) as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    if attempt == max_retries - 1:
                        logger.error(f"Function {func.__name__} failed after {max_retries} attempts")
                        raise
                    time.sleep(RETRY_DELAY)
        return wrapper
    return decorator

def transfer_to_output_machine(
    local_file_path: str, 
    output_host: str, 
    output_username: str, 
    output_password: str, 
    remote_output_path: str
) -> bool:
    """Transfer file to output machine"""
    try:
        # Connect to output machine
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            output_host, 
            username=output_username, 
            password=output_password,
            timeout=CONNECTION_TIMEOUT
        )
        
        sftp = ssh.open_sftp()
        
        # Ensure remote output directory exists
        try:
            sftp.mkdir(remote_output_path)
        except:
            logger.info(f"Directory {remote_output_path} might already exist")
        
        # Transfer file
        remote_file_path = os.path.join(remote_output_path, os.path.basename(local_file_path))
        sftp.put(local_file_path, remote_file_path)
        logger.info(f"Transferred {local_file_path} to {remote_file_path}")
        
        sftp.close()
        ssh.close()
        return True
    
    except Exception as e:
        logger.error(f"Error transferring to output machine: {e}")
        return False

@retry_on_network_error()
def transfer_execute_download(
    host: str, 
    username: str, 
    password: str,
    remote_path:str, 
    local_script_path: str, 
    local_output_path: str, 
    command: str,
    output_host: Optional[str] = None,
    output_username: Optional[str] = None,
    output_password: Optional[str] = None,
    remote_output_path: Optional[str] = None,
    log_file: str = 'transfer_log.txt'
) -> bool:
    """Enhanced transfer, execute, and download function with robust command waiting"""
    try:
        logger.info(f"Starting execution on machine {host}")

        # Secure SSH Connection
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        ssh.connect(
            host, 
            username=username, 
            password=password, 
            timeout=CONNECTION_TIMEOUT
        )

        sftp = ssh.open_sftp()

        # Create remote directory for script
        remote_home = remote_path
        remote_collection_dir = os.path.join(remote_home, 'Collection')
        
        try:
            sftp.mkdir(remote_collection_dir)
        except:
            logger.info(f"Directory {remote_collection_dir} might already exist")

        # Transfer script
        remote_script_path = os.path.join(remote_collection_dir, 'Collection.zip')
        sftp.put(local_script_path, remote_script_path)
        logger.info(f"Transferred script to {remote_script_path}")

        # Unzip the script with error checking
        unzip_channel = ssh.get_transport().open_session()
        unzip_command = f'unzip -o {remote_script_path} -d {remote_collection_dir}'
        unzip_channel.exec_command(unzip_command)
        logger.info(f"Executing unzip: {unzip_command}")
        
        unzip_exit_status = unzip_channel.recv_exit_status()
        unzip_stdout = unzip_channel.recv(4096).decode('utf-8', errors='ignore')
        unzip_stderr = unzip_channel.recv_stderr(4096).decode('utf-8', errors='ignore')
        
        if unzip_exit_status != 0:
            logger.error(f"Unzip failed (Status {unzip_exit_status}):")
            logger.error(f"STDOUT: {unzip_stdout}")
            logger.error(f"STDERR: {unzip_stderr}")
            raise Exception("Failed to unzip script")

        # Set permissions to 777 for the entire Collection folder
        perm_channel = ssh.get_transport().open_session()
        perm_command = f'chmod -R 777 {remote_collection_dir}'
        perm_channel.exec_command(perm_command)
        logger.info(f"Executing permission change: {perm_command}")
        
        perm_exit_status = perm_channel.recv_exit_status()
        perm_stdout = perm_channel.recv(4096).decode('utf-8', errors='ignore')
        perm_stderr = perm_channel.recv_stderr(4096).decode('utf-8', errors='ignore')
        
        if perm_exit_status != 0:
            logger.error(f"Permission setting failed (Status {perm_exit_status}):")
            logger.error(f"STDOUT: {perm_stdout}")
            logger.error(f"STDERR: {perm_stderr}")
            raise Exception("Failed to set permissions")

        # Prepare script execution
        collection_bin_path = os.path.join(remote_collection_dir, 'Collection/bin/')

        # Change to the bin directory and execute the command
        full_command = f'cd {collection_bin_path} && {command}'
        
        logger.info(f"Executing main command: {full_command}")
        
        # Create a new channel for command execution
        exec_channel = ssh.get_transport().open_session()
        exec_channel.exec_command(full_command)
        
        # Capture and log full output
        stdout_output = ""
        stderr_output = ""
        
        # Read stdout
        while not exec_channel.exit_status_ready():
            if exec_channel.recv_ready():
                chunk = exec_channel.recv(4096).decode('utf-8', errors='ignore')
                stdout_output += chunk
                logger.info(f"STDOUT: {chunk}")
            time.sleep(0.1)
        
        # Read stderr
        while exec_channel.recv_stderr_ready():
            chunk = exec_channel.recv_stderr(4096).decode('utf-8', errors='ignore')
            stderr_output += chunk
            logger.error(f"STDERR: {chunk}")
        
        # Get final exit status
        exit_status = exec_channel.recv_exit_status()
        
        if exit_status == 0:
            logger.info("Command executed successfully")
        else:
            logger.error(f"Command execution failed with status {exit_status}")
            logger.error(f"Full STDOUT: {stdout_output}")
            logger.error(f"Full STDERR: {stderr_output}")
            return False

        # Find and transfer output files
        remote_output_dir = os.path.join(collection_bin_path, 'output')
        
        # Ensure local output directory exists
        os.makedirs(local_output_path, exist_ok=True)

        # Transfer Collection file
        stdin, stdout, stderr = ssh.exec_command(f'ls {remote_output_dir}/Collection-*.tar.bz2')
        collection_files = stdout.read().decode().strip().split()
        
        transferred_files = []
        for remote_collection_file in collection_files:
            local_collection_file = os.path.join(local_output_path, f'Collection_{host}_{username}.tar')
            sftp.get(remote_collection_file, local_collection_file)
            logger.info(f"Transferred Collection file to {local_collection_file}")
            transferred_files.append(local_collection_file)

        # Transfer debug file
        stdin, stdout, stderr = ssh.exec_command(f'ls {remote_output_dir}/debug_*.tar.bz2')
        debug_files = stdout.read().decode().strip().split()
        
        for remote_debug_file in debug_files:
            local_debug_file = os.path.join(local_output_path, f'debug_{host}_{username}.tar')
            sftp.get(remote_debug_file, local_debug_file)
            logger.info(f"Transferred debug file to {local_debug_file}")
            transferred_files.append(local_debug_file)

        sftp.close()
        ssh.close()
        
        # Transfer to output machine if specified
        if output_host and output_username and output_password and remote_output_path:
            for local_file in transferred_files:
                transfer_to_output_machine(
                    local_file, 
                    output_host, 
                    output_username, 
                    output_password, 
                    remote_output_path
                )
        
        logger.info(f"Finished execution on machine {host}")
        return True

    except Exception as e:
        logger.error(f"Error processing {host}: {e}")
        return False

def read_config_files(config_directory: str) -> List[Dict[str, str]]:
    """
    Read configuration files with the new format:
    1. First file: command
    2. Subsequent files: host, username, password, local output path, 
       optional: output host, output username, output password, remote output path
    """
    configs = []
    config_files = sorted(os.listdir(config_directory))
    
    if not config_files:
        logger.error("No configuration files found")
        return configs

    # Read command from the first config file
    first_config_path = os.path.join(config_directory, config_files[0])
    try:
        with open(first_config_path, 'r') as f:
            command = f.readline().strip()
    except Exception as e:
        logger.error(f"Error reading command: {e}")
        return configs

    # Process remaining config files
    for config_file in config_files[1:]:
        config_path = os.path.join(config_directory, config_file)
        try:
            with open(config_path, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    config = {
                        'host': parts[0],
                        'username': parts[1],
                        'password': parts[2],
                        'remote_path':parts[3],
                        'local_output_path': parts[4],
                        'command': command
                    }
                    
                    # Optional output machine details (if provided)
                    if len(parts) >= 7:
                        config.update({
                            'output_host': parts[5],
                            'output_username': parts[6],
                            'output_password': parts[7],
                            'remote_output_path': parts[8] if len(parts) > 7 else '/home/output'
                        })
                    
                    configs.append(config)
        except Exception as e:
            logger.error(f"Error reading config file {config_file}: {e}")

    return configs

def process_server_configurations(
    config_directory: str, 
    local_script_path: str, 
    log_file: str
) -> List[bool]:
    """Parallel processing of server configurations"""
    results = []
    
    # Read configurations
    configs = read_config_files(config_directory)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(
                transfer_execute_download,
                config['host'], 
                config['username'], 
                config['password'],
                config['remote_path'], 
                local_script_path, 
                config['local_output_path'], 
                config['command'],
                config.get('output_host'),
                config.get('output_username'),
                config.get('output_password'),
                config.get('remote_output_path'),
                log_file
            )
            for config in configs
        ]
        
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    
    return results

def main():
    config_directory = 'configs'
    log_file = 'transfer_log.txt'
    local_script_path = os.path.join(os.path.abspath(os.getcwd()), 'Collection.zip')

    # Setup logging
    global logger
    logger = setup_logging(log_file)

    # Execute server configurations
    try:
        results = process_server_configurations(
            config_directory, 
            local_script_path, 
            log_file
        )

        # Log summary
        successful = sum(results)
        total = len(results)
        logger.info(f"Execution Summary: {successful}/{total} servers processed successfully")

    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")

if __name__ == "__main__":
    main()