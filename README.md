# DROPS: Managing Resource Pools in a Large-Scale Commercial Serverless Platform

This repository accompanies the following research paper:
*DROPS: Managing Resource Pools in a Large-Scale Commercial Serverless Platform* submitted to EuroSys 2026. 


This repository has everything you need to verify the paper’s claims and reproduce the figures. This includes the simulator code of the serverless platform, implementation of various resource optimization methods, training scripts, and plotting scripts.


## Repository Structure

* `drops/` — the source code of the simulator of our serverless platform, along with an implementation of DROPS algorithm as well as all other alternatives we discuss in the paper (i.e., production, predictive, reactive, and predictive-reactive). 


* `setup-scripts/` — contains useful bash scripts to facilitate setting up the environment.

  * `install-dep.sh` — installs the simulator, training, and plotting dependencies.
  * `fetch-traces.sh` — downloads the traces used in the paper and extracts it to the right path (`./traces/`)
  * `build.sh` — builds the simulator from source files

* `experiments/` — contains scripts and config files to automate running experiments and generating the figures in the paper.

* `training/` — optional: contains scripts to install training dependencies and retrain models to regenerate the predicted trace.

* `traces/` — will be created by `fetch-traces.sh` script (contains input traces, not checked in).


## Software & Hardware Requirements

The simulator code is written in C# and requires .NET Runtime 9.0. The simulator code is tested on Ubuntu 22.04. However, it should work on Windows and macOS.

The automation scripts are tested on Ubuntu 22.04. We recommend using a machine with **Ubuntu 22.04**.


## Reproducing Our Results

You can reproduce all the major claims from the paper with this repo. 

### Step 0: Prerequisites (≈2 human minutes + 5 compute-minutes)

#### A. Installing dependencies:

**For Ubuntu 22.04, we provide a bash script to install all dependencies:**
```bash
cd <repo-root>
./setup-scripts/install-dep.sh 
```

Otherwise you need to install dependencies manually. The dependencies are installable on Linux, Windows and macOS.

<!-- **If you’re installing on other platforms, you need:** -->

1. **For the simulator:** `.NET SDK 9.0`
2. **For plotting:** `Python3` + `matplotlib`, `pandas`, `numpy`

<!-- 3. **Training (optional):** [AutoGluon](https://auto.gluon.ai/stable/install.html), `pandas`, `numpy` -->

#### B. Building the simulator:
**To build the simulator from source run:**
```bash
cd ./setup-scripts
./build.sh 
```

### Step 1: Fetch the traces (≈1 human minute + 2–5 compute-minutes)

We provide a script that pulls the traces (\~800 MB) from Google Drive and unpacks into `./traces/`. The traces are password protected. We will provide the passwords to the AEC.

**1. Set the correct password in:`setup-scripts/fetch-traces.sh`**
```bash
# top of setup-scripts/fetch-traces.sh
ZIP_PASSWORD="changeme"   # <-- replace with the real password
```

**2. Run:**

```bash
setup-scripts/fetch-traces.sh
```

after it finishes, `./traces/` should contain these traces (CSV files):

```
1_day_0_day_offset.csv
1_day_1_day_offset.csv
...
1_day_6_day_offset.csv

1_week_0_day.csv
...
1_week_6_day.csv

lifecycles_eastus.20241101-1.csv
lifecycles_eastus.20241101-14.csv
predicited_trace.csv
trace_eastus.20241101-1.csv
trace_eastus.20241101-7.csv
trace_eastus.20241101-14.csv
trace_eastus.20241107-14.csv
vm_creation_latency.csv
```

### Step 2: Running a minimal test [≈1 human minute + 5 compute-minutes]

Perform the following commands to run a test example. Note that you must have fetched the traces before running this example. 

```bash
# must be in this directory
cd experiments

# run all experiments 
./scripts/test-exp.sh

```

After the script finishes, you should find two figures: 
`/experiments/test-exp/test_failure_rate.pdf`
`/experiments/test-exp/test_latency.pdf`


### Step 2: Running experiments [≈1 human minutes + 2.5 compute-hours]

Perform the following commands to run all experiments and generates **Figures 6, 7, 10, 11, and 12** from the paper. Figures are generated in the `./experiments` folder. These figures reproduce our results and support the main claims in the paper. 


```bash
# must be in this directory
cd ./experiments

# run all exps 
./run-all.sh

```

Few notes about runnig the experiment:
- Figures are generated as PDF files
- The raw results of each experiment is stored in a separate folder (e.g., `\experiments\fig6`)
- The log of running each experiment is stored in its folder (e.g., `\experiments\fig6\log.txt`)  


#### Running a single experiment
You can use the following commands to run a single experiment. This will run the experiment, collect its results, and generate its figures.  

```bash
# must be in this directory
cd ./experiments

# run a single experiment 
# fig6.sh can be replaced with any of:
# fig7.sh, fig10a.sh, fig10b.sh, fig11.sh, fig12.sh 
./scripts/fig6.sh 

```

### Step 3 (optional): Producing the predicted trace  [≈2 human mins + up to 48 compute-hours]

Note that we include the predicted trace (`predicted_trace.csv`) in our traces, so you **don’t** have to retrain the models to reproduce our results. 

However, if you want to train the models from scratch, execute the following commands (**Training requires a machine with at least 128 GB of RAM**):



```bash
cd training
# install training dependencies
./training-dep.sh                 

# generates training/best_models/predicted_trace.csv    
python3 train.py                  
cp best_models/predicited_trace.csv ../traces/
```


## Reusing the simulator with other traces

The simulator enables evaluating different resource optimization methods under different traces. The simulator accepts two traces:
- **container-allocation trace**: a time series of container-allocation requests. `/drops/sampleTraces/allocation_trace.csv` shows an the format of this trace.

- **lifecycle trace**: the latency of every stage in the container lifecycle. `/drops/sampleTraces/lifecycle_trace.csv` shows an the format of this trace.

You must transform your traces to the accepted formats by the simulator.


