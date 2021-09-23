### Example README

### Simple test
1. create instance based on Ubuntu 18 in AWS in e.g. `eu-west-1`
2. `pip install -r requirements.txt`
3. `pip install -r requirements-dev.txt`
4. install Docker, if you don't have it already:
```shell
sudo apt -y install apt-transport-https ca-certificates curl gnupg-agent software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt update
sudo apt -y install docker-ce docker-ce-cli containerd.io
sudo groupadd docker; sudo gpasswd -a $USER docker
```
4. `cortex cluster up example/cluster.yaml`
5. install local postgresql DB:
```bash
sudo su postgres;
psql postgres postgres;
create database cortex_test;
create role cortex_test login password 'cortex_test';
grant all privileges on database cortex_test to cortex_test;
```

You may need to configure also
```bash

vi /etc/postgresql/11/main/pg_hba.conf
# change a matching line into following to allow localhost network access
# host    all             all             127.0.0.1/32            trust

sudo systemctl restart postgresql;
```
5. Optionally create a CSC S3 bucket or use an existing one
6. Optionally build and push `base_docker_image` to AWS ECR to get its ID or use you own
7. set env vars according to CSC bucket info:
```shell
CSC_BUCKET_NAME=<bucket name>
CSC_S3_SSE_KEY=<SSE KEY corresponding to the bucket name>
DEFAULT_BASE_DOCKER_IMAGE=<Docker image ID to use as default base image>
```
7. run `integration_tests.py`


### Local base image test
Edit the `download_source_code.py` script to load source code from disk and not S3 
because the locally run docker image will not have AWS credentials. 

Build docker image:
```
docker build . -t predictor
```

Run docker image:
```
docker run -p 8080:8080 predictor
```

Make request to the running container:
```
curl -X POST -H "Content-Type: application/json" -d '{"msg": "hello world"}' localhost:8080
```

Kill all docker containers (or just kill this one):
```
docker stop `docker ps -q`
```
