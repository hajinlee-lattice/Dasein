FROM ubuntu:latest
RUN apt-get update -y
RUN apt-get install -y python-pip python-dev build-essential
RUN pip install --upgrade pip
RUN pip install Flask

RUN echo invalidate cache 1466405081 > /dev/null
COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt
EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["app.py"]