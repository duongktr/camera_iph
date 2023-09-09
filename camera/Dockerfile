FROM python:3.8-slim

WORKDIR /app

COPY . .

RUN apt-get update -y && apt-get install -y telnet curl nano vim

RUN python3 -m pip install -U pip && python3 -m pip install -r requirements.txt

CMD [ "python3", "app.py" ]
