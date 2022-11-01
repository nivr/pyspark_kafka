SET SPARK_VERSION="3.3.1"
SET HADOOP_VERSION="3"
SET JUPYTERLAB_VERSION="3.5.0"

REM # -- Building the Images

docker build -f cluster-base.Dockerfile -t cluster-base .

pause

docker build --build-arg spark_version="%SPARK_VERSION%" --build-arg hadoop_version="%HADOOP_VERSION%" -f spark-base.Dockerfile -t spark-base .

pause

docker build -f spark-master.Dockerfile -t spark-master .

pause

docker build -f spark-worker.Dockerfile -t spark-worker .

pause

docker build --build-arg spark_version="%SPARK_VERSION%" --build-arg jupyterlab_version="%JUPYTERLAB_VERSION%" -f jupyterlab.Dockerfile -t jupyterlab .

pause

REM Local copy of Notebooks and job-submit scripts outside Git change tracking
mkdir -p ./local/notebooks
xcopy .\notebooks\* .\local\notebooks\* /s /e /h
