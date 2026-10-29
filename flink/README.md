# Guide for Run Kafka and Flink

This guide takes you from a Windows machine with nothing installed to a full run of the water pipeline: the Kafka part that publishes the data, and the Flink part that cleans it and raises alerts. Follow it top to bottom and you should not need to ask anyone anything. Every step says what it does and why we do it.

A complete run takes about 20 minutes once everything is installed. The first time you run it, add extra time for downloading and building the Docker images, which depends on your internet speed.

## What this project does

The system watches water flowing into six districts and looks for problems such as bursts, leaks and supply failures.

- The generator creates simulated sensor and meter readings. It runs a real hydraulic model, so flow and pressure behave in a believable way, and a few incidents are planted on purpose.
- Kafka carries those readings as live streams.
- Flink reads the district stream, cleans it, builds 15 minute summaries per district, compares each district with the others, and writes alerts back to Kafka.
- At the end, an evaluation script compares our alerts with the list of planted incidents to see how many we caught.

Everything runs inside Docker containers, so you do not install Kafka or Flink yourself.

## What you need before starting

A Windows 10 or 11 machine with:

- At least 16 GB of RAM. 8 GB can work but Flink may crash, see the memory note near the end.
- At least 20 GB of free disk space for images, volumes and the data.
- A stable internet connection for the first setup.
- Virtualization enabled in the BIOS. Most machines have it on already. You can check in Task Manager under the Performance tab, where it should say Virtualization: Enabled.

The ports 8080, 8081, 9000 and 9001 must be free. They are used by Kafka UI, the Flink dashboard and MinIO.

## Why we use WSL and not plain Windows

The helper scripts in this project are bash scripts. They do not run in PowerShell or CMD. The setup that was tested end to end is Ubuntu on WSL2 together with Docker Desktop, and Docker Desktop on Windows uses WSL2 under the hood anyway. So installing both is the shortest path, and it is the one this guide uses.

From the moment Ubuntu is installed, every command below is typed in the Ubuntu terminal, not in PowerShell.

## Installing the tools

### Ubuntu on WSL2

Open PowerShell as Administrator and run:

```powershell
wsl --install -d Ubuntu
```

Restart the computer when it asks. After the restart Ubuntu opens by itself and asks you to create a username and a password. Choose anything you will remember. This is your Linux user and it has nothing to do with your Windows account.

To confirm it worked, run this in PowerShell:

```powershell
wsl -l -v
```

You should see Ubuntu listed with version 2.

### Docker Desktop

Download Docker Desktop from the official Docker website and install it. During or after installation, open Settings and check two things:

- Under General, the option to use the WSL 2 based engine is on.
- Under Resources, then WSL integration, the switch next to Ubuntu is on. Press Apply and restart.

Start Docker Desktop and wait until it says the engine is running. Nothing below works while it is still starting.

### Checking Docker from Ubuntu

Open the Ubuntu terminal and run:

```bash
docker --version
docker compose version
docker info
```

The first two print versions. The third prints a long block of information. If `docker info` gives an error, Docker Desktop is not running yet, or the WSL integration switch is off.

### Git, curl and Python

Ubuntu usually has these, but install them to be sure:

```bash
sudo apt update
sudo apt install -y git curl python3
```

The scripts use `curl` and `python3` to read the Flink status, and the evaluation uses Python. You do not need pip or a virtual environment for this guide, because the evaluation script only uses the standard library.

## Getting the project

Clone the repository inside the Linux file system, in your home folder:

```bash
cd ~
git clone <repository-url>
cd egypt-water-intelligence
git checkout <branch-name>
```

Replace the repository URL with the one the team shared, and the branch name with the branch that contains the Flink work. If the Flink work was already merged into main, skip the checkout.

Do not clone under `/mnt/c/...`. That path is the Windows disk seen from Linux. It is slow, and it can break file permissions and line endings, which makes the scripts fail in confusing ways.

## Making the scripts runnable

Git sometimes changes line endings or drops the execute permission. Run these once to be safe:

```bash
sed -i 's/\r$//' flink/scripts/*.sh
chmod +x flink/scripts/*.sh
```

The first line removes Windows style line endings. If you skip it and the line endings are wrong, you will see an error like `bad interpreter` or `$'\r': command not found`. The second line gives the scripts permission to run.

## The data

This is the part people get stuck on, so read it fully.

The project does not read real meters. It reads simulated readings that were generated once by the generator and saved in a folder called `output`. That folder is not stored in Git, because it holds more than 1.6 million events and would make the repository huge. That means a fresh clone does not contain any data, and you have to put it there yourself.

The folder you need is named exactly:

```
run-72b96af88a342375
```

and it must end up at this location inside the project:

```
egypt-water-intelligence/Generator/output/run-72b96af88a342375/
```

The teammate who built the Kafka part sends this folder directly, for example through WhatsApp or a shared drive. You do not need to put a path in any script or config file. The Docker setup already links the host folder `Generator/output` to `/app/output` inside the containers, and the scripts already point to the run folder by name. If the folder sits in the right place, everything finds it.

### Copying the folder into WSL

If you received it on Windows and unzipped it, say into Downloads, copy it into Ubuntu like this:

```bash
mkdir -p ~/egypt-water-intelligence/Generator/output
cp -r /mnt/c/Users/<your-windows-username>/Downloads/run-72b96af88a342375 ~/egypt-water-intelligence/Generator/output/
```

You can also open File Explorer, type `\\wsl$\Ubuntu\home\<your-linux-username>\egypt-water-intelligence\Generator` in the address bar and drag the folder in. Make sure you end up with `output/run-72b96af88a342375` and not `output/run-72b96af88a342375/run-72b96af88a342375`, because an extra level is a common mistake.

### What should be inside

```bash
ls Generator/output/run-72b96af88a342375/
```

You should see these items:

| Item | What it is |
|---|---|
| `delivery/` | The streams with injected faults. This is what we publish to Kafka and what Flink processes. |
| `public/` | The clean version of the same streams, used as a reference. |
| `restricted/` | The answer key, which lists the planted incidents and faults. Flink never reads it. Only the evaluation script does, at the very end. |
| `config.yml` | The settings the data was generated with. |
| `run-manifest.json` | Record counts for the run. |
| `validation-report.json` | The generator's own checks. |
| `_RUN_COMPLETE` | A marker file that proves the generation finished. |

All three folders have to be there. If `restricted/` is missing the evaluation will fail, and if `delivery/` is missing there is nothing to publish.

### Checking the data is complete

```bash
ls Generator/output/run-72b96af88a342375/_RUN_COMPLETE
wc -l Generator/output/run-72b96af88a342375/delivery/*.ndjson
```

The line counts should be 216,071 for the district sensors file and 3,600,454 for the meter readings file. Each file has more lines than its clean copy in `public/` because duplicate events were injected on purpose (71 district, 454 meter). If the numbers are smaller, the copy was cut off and you need to copy it again.

### If you do not have the folder

You can generate it yourself. The seed is fixed, so it produces the same data with the same folder name:

```bash
docker compose build generator
docker compose run --rm generator generate --config /app/pipeline-configs/pipeline-large.yml
```

This runs the hydraulic simulation and can take several minutes. If the folder that appears has a different name, the configuration was changed. In that case set `RUN_DIR=/app/output/<new-name>` before running the live demo, and pass the new folder as an argument to the evaluation script. Receiving the original folder is simpler and guarantees identical results, so prefer that.

## Starting the infrastructure

All commands from here run from the project root, `~/egypt-water-intelligence`.

### Kafka, MinIO and Kafka UI

```bash
docker compose up -d kafka minio kafka-ui
docker compose ps
```

This starts the message broker, the object storage and a web page for looking inside Kafka. The first time, Docker downloads the images, which takes a few minutes. Run `docker compose ps` again until `kafka` and `minio` show as healthy.

You can open these in your browser:

- Kafka UI at http://localhost:8080
- MinIO console at http://localhost:9001, user `wateradmin`, password `wateradminsecret`

Kafka is not exposed to Windows on a port. Only containers inside the Docker network reach it, using the address `kafka:9092`. That is why all Kafka commands go through `docker compose`.

### Creating the topics and buckets

```bash
docker compose run --rm init
```

A fresh Kafka has no topics and a fresh MinIO has no buckets. This command creates the two raw topics, `water.district-sensors.v1` and `water.meter-readings.v1`, and the three buckets. The first run builds the generator image, so it takes a while. When it finishes without an error you are done with this step. It does not load any data, it only prepares the empty containers.

### Flink

```bash
docker compose up -d flink-jobmanager flink-taskmanager
```

This starts the two Flink services. They are defined in `docker-compose.override.yml`, which Docker Compose reads automatically as long as it sits next to the main compose file. The first run builds the Flink image, which also takes a few minutes.

Open http://localhost:8081. You should see the Flink dashboard with one task manager and four task slots. If the page does not load, wait a minute and refresh, since Flink needs a short while to start.

## Checks before the run

Run through this list. If every line matches, the live demo is safe to start.

| Check | Command | Expected |
|---|---|---|
| Docker works | `docker compose version` | prints a version |
| Services are healthy | `docker compose ps` | kafka and minio healthy, flink services running |
| Flink is up | open http://localhost:8081 | one task manager, four slots |
| Data is in place | `ls Generator/output/run-72b96af88a342375/_RUN_COMPLETE` | the file is listed |
| No job is running | `docker compose exec flink-jobmanager flink list -r` | `No running jobs.` |

## Running the live demo

```bash
flink/scripts/live_demo.sh
```

This one command does both the Kafka part and the Flink part together. It first resets the topics and starts the four Flink jobs on empty topics. Then it publishes the delivery data to Kafka while the jobs are already running, so Flink processes the data as it arrives, the way it would in real life. You do not run the publish command yourself, the script already does it.

The script deletes and recreates the raw topics and all the downstream topics every time you run it. That is intended, it gives you a clean start. It also refuses to run if Flink jobs are still running from before, so stop them first if you need to repeat the run.

While it works you will see progress lines like this one:

```
[publishing] raw=... clean=... features=... alerts=...
```

The numbers grow as data moves through the pipeline. When publishing ends, the script waits until the output counts stop changing, prints the health of each job and a summary of every topic, and leaves the jobs running.

Expect around 20 minutes from start to finish. Do not close the terminal and do not put the laptop to sleep. If the machine is under heavy load it can take longer. You can watch the jobs live in the Flink dashboard at http://localhost:8081 and the topics filling up in Kafka UI.

## What you should see at the end

> **Measured on the current dataset** (`run-72b96af88a342375`, `configs/pipeline-large.yml`: 1,500 meters, 13 planted scenarios), through the orchestrated pipeline (see `docs/`).

The summary at the bottom of the script should match these numbers:

| Topic | Expected rows |
|---|---|
| `water.district-sensors.v1`, raw district | 216,071 |
| `water.meter-readings.v1`, raw meter | 3,600,454 |
| `water.district-sensors.clean.v1` | 216,000 |
| `water.dead-letter.district.v1` | 0 |
| `water.district-features.15m.v1` | 14,394 |
| `water.district-relative.15m.v1` | 14,388 |
| `water.district-alerts.v1` | 140 |

The meter topic is only filled and not processed, because the meter stream belongs to the batch side of the project and not to Flink.

The features topic has 14,394 rows because there are 2,399 fifteen minute windows for each of the 6 districts. The last window of the data never closes in a finite replay, so it is not emitted. The alerts are 140 in total: 15 incidents found by the main rules (an open and a closed row each), 2 level shift alerts, and 108 data quality alerts.

Each job should also report the state `RUNNING` in the health section. You may see a few failed checkpoints reported, for example `completed 0 failed 1`. That is harmless. It happens when a checkpoint is triggered while a job is still starting, and the output is not affected.

## Evaluating the alerts

> **Measured on the current dataset** (`run-72b96af88a342375`, `configs/pipeline-large.yml`: 1,500 meters, 13 planted scenarios), through the orchestrated pipeline (see `docs/`).

```bash
PYTHON=python3 flink/scripts/evaluate.sh
```

This reads the alerts topic and compares it with the answer key in the `restricted` folder. It only reads, it changes nothing. The `PYTHON=python3` part is needed because Ubuntu has no command called `python`, only `python3`.

The expected result:

| Planted incident | Detected | Alert type | Delay |
|---|---|---|---|
| burst in D001 | yes | flow_anomaly | 30 min |
| gradual leak in D002 | yes | flow_anomaly | 30 min |
| supply disturbance in D003 | yes | supply_disturbance | 60 min |
| demand surge in D001 | yes | flow_anomaly | 30 min |
| burst in D006 | yes | flow_anomaly | 30 min |
| supply disturbance in D002 | yes | supply_disturbance | 45 min |
| persistent loss in D005 | yes | flow_level_shift | about 1 day |
| demand surge in D004 | yes | flow_anomaly | 30 min |
| burst in D004 | yes | flow_anomaly | 30 min |
| gradual leak in D003 (one day) | yes | flow_anomaly | 6.5 hours |
| burst in D002 | yes | flow_anomaly | 30 min |
| demand surge in D006 | yes | flow_anomaly | 30 min |
| background loss in D006 | not detectable | (credited by the matching rule) | |

Below the table the script prints the summary:

- Recall is 12 out of 12 on the incidents that can be detected from district data.
- There is 1 false alarm, so precision is 0.94: a level shift alert on D004 on Jan 21, when the D005 persistent loss ends and lowers the peer median D004 is compared with.
- Data quality shows 108 faults, 108 alerted.

The D006 loss is not a bug. It is a leak that exists from the very first minute, so the flow never changes and there is nothing to detect from the district stream alone. Only comparing against the customer meters can reveal it, and that is the batch side's job. During Friday prayer, when demand drops, the D006 leak becomes a larger share of the flow and briefly crosses the 10% threshold; the matching rule credits those alerts to the D006 scenario, which is why it is not counted as detectable.

Also note that the burst and the demand surge in D001 look identical in district data. Flink reports both as a flow anomaly. Telling them apart needs the meter volumes.

## Stopping

```bash
flink/scripts/stop_all.sh
```

This stops every running Flink job cleanly and saves a savepoint, which is a snapshot of the job state. The savepoint paths are appended to `flink/scripts/savepoints.log`. Use this script whenever you are done, instead of just closing Docker, because it avoids leaving duplicate rows behind.

To shut everything down afterwards:

```bash
docker compose down
```

This stops the containers but keeps the Kafka and MinIO data. Never add `-v` unless you really want to erase everything, because it deletes the volumes, including all Kafka data, stored events and Flink checkpoints.

## What each script does

All of them are in `flink/scripts/`.

**common.sh** is not run on its own. The other scripts load it. It defines the topic names and small helper functions, such as counting the rows in a topic, creating and deleting topics, submitting a Flink job, and printing a job's health.

**live_demo.sh** is the main one. It resets all topics, starts the four jobs, publishes the data while they run, waits for the outputs to settle, and prints the counts and health. The jobs stay running afterwards.

> **With the orchestrated stack, do not run `live_demo.sh` or `start_all.sh --reset`.** They delete Kafka topics that the archive and the publication record depend on. Use `.un.ps1 demo` instead, which replays into a separate `demo.` namespace (see `docs/RUNNING.md`).

**start_all.sh** is an alternative to the live demo. The data must already be sitting in the raw topic. It runs the four jobs one after another, waits for each job's output to stop growing, prints the counts and cancels the job before starting the next. This needs less memory because only one job runs at a time. Use it with `--reset` for a clean run. It also takes about ten minutes.

**evaluate.sh** reads the alerts topic and compares the alerts with the answer key. It is read only.

**stop_all.sh** stops all running jobs with a savepoint and writes the savepoint paths to a log file.

The `flink/analysis/` folder holds the Python scripts used earlier to study the data, choose the thresholds and test the rules. They are not needed for the run above.

## The four jobs in short

The live demo starts these jobs in order. Each one reads the topic written by the one before it.

| Job | What it does |
|---|---|
| Job 2, validation | Checks every district event, sends broken ones to a dead letter topic with the reason, and removes duplicate event ids. A missing pressure is kept as empty and never turned into zero. |
| Job 3a, features | Groups the clean events into 15 minute windows per district and computes average flow, volume, minimum pressure and quality counters. |
| Job 3b, alerts | Compares each district with the median of the other five and raises alerts for flow anomalies, supply disturbances and data quality problems. |
| Job 3c, level shift | Looks at each district's daily level against its own last six days and raises an alert when it rises by more than 3 percent. |

## When something goes wrong

**The script says `bad interpreter` or `command not found` with a strange character.** The line endings are wrong. Run the `sed` command from the section on making the scripts runnable.

**Permission denied when starting a script.** Run `chmod +x flink/scripts/*.sh`.

**`python: command not found`.** Put `PYTHON=python3` before the evaluation command.

**The raw topic is empty, or the publish step fails.** Check that the run folder exists at the right path and that it has the delivery folder. Also confirm `init` was run, because without it the topics do not exist.

**A port is already in use.** Another program is using 8080, 8081, 9000 or 9001. Close it, or stop old containers with `docker compose down`.

**The script says jobs are already running.** Run `flink/scripts/stop_all.sh` first.

**Flink containers keep dying or a job goes to `RESTARTING`.** This is almost always memory. Docker on WSL2 uses about half of the machine's RAM by default. Create a file at `C:\Users\<your-windows-username>\.wslconfig` with these lines:

```
[wsl2]
memory=8GB
```

Choose a value your machine can spare, then run `wsl --shutdown` in PowerShell and start Docker Desktop again. If a job is stuck in `RESTARTING`, read the reason before cancelling it:

```bash
curl -s http://localhost:8081/jobs/<job-id>/exceptions
```

If memory is tight, use `flink/scripts/start_all.sh --reset` instead of the live demo, since it keeps only one job alive at a time.

**The numbers differ slightly from the tables above.** Check that you used the delivery data and that no old output was left in the topics. Running the live demo again from a clean state, after stopping the jobs, gives a fresh result.

## Quick recap

For someone who already has everything installed, the full sequence is:

```bash
cd ~/egypt-water-intelligence
docker compose up -d kafka minio kafka-ui
docker compose run --rm init
docker compose up -d flink-jobmanager flink-taskmanager
ls Generator/output/run-72b96af88a342375/_RUN_COMPLETE
flink/scripts/live_demo.sh
PYTHON=python3 flink/scripts/evaluate.sh
flink/scripts/stop_all.sh
```
