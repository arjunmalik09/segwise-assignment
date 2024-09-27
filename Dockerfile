FROM apache/spark-py:v3.4.0

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# download dataset
RUN wget "https://drive.google.com/file/d/1k2kZ4BfnvfyeLR0jnuW3OX2EB-XGQkw9/view?usp=drive_link"
RUN tar -xf google-play-dataset-by-tapivedotcom.csv.tar

CMD [ "python", "./main.py", "google-play-dataset-by-tapivedotcom.csv" ]
