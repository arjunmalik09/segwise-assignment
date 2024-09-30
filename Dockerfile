FROM spark:python3

COPY requirements.txt ./
USER root
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY . .

# download dataset
# RUN wget "https://drive.google.com/file/d/1k2kZ4BfnvfyeLR0jnuW3OX2EB-XGQkw9/view?usp=drive_link"
# RUN tar -xf google-play-dataset-by-tapivedotcom.csv.tar

RUN tar -xf google-play-dataset-by-tapivedotcom-truncated.csv.tar
CMD [ "python3", "./main.py", "google-play-dataset-by-tapivedotcom-truncated.csv" ]
