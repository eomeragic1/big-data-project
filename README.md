# big-data-project

salloc -c 12 --mem=64gb --time=03:00:00 --job-name="BigDataProject" // request as many resources as you want

// lets say you got wn105 node, then do

ssh wn105
conda activate bd39
jupyter-lab --no-browser --port=8892 --ip=0.0.0.0


// open new terminal s  ession and do, keep the terminal open after you press enter
ssh -N -f -L 8892:wn105.arnes.si:8892 eo3031@hpc-login.arnes.si

// you can open 127.0.0.1:8892 in browser now to open jupyterlab running in cluster (or better copy the link that jupyter provided when it started the instance)