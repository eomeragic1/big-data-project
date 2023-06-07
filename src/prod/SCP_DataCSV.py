from box import Box
from paramiko import SSHClient
from scp import SCPClient

if __name__ == '__main__':
    # Read configuration files
    secrets = Box.from_yaml(filename='config/secrets.yaml')
    config = Box.from_yaml(filename='config/config.yaml')

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(hostname=secrets['slurm_cluster']['hostname'],
                username=secrets['slurm_cluster']['username'],
                password=secrets['slurm_cluster']['password'])

    # SCPCLient takes a paramiko transport as an argument
    scp = SCPClient(ssh.get_transport())

    # Uploading the 'test' directory with its content in the
    # '/home/user/dump' remote directory
    scp.put(files='data/csv/augmentation',
            recursive=True,
            remote_path=config['environment']['hpc']['data_augmentation_dir'],)

    scp.close()
