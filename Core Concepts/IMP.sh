cd ~/Documents/GithubRepos/Spark_DE

source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install pyspark==3.5.4 jupyter ipykernel

python -c "import pyspark; print(pyspark.__version__)"

python -m ipykernel install --user \
  --name spark_de_venv \
  --display-name "Python (.venv) Spark_DE"


spark-submit --version


