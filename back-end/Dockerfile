FROM python:3.10.10
EXPOSE 5000
WORKDIR /app
RUN pip install flask
RUN pip install aiohttp
RUN pip install datetime
RUN pip install flask-cors
RUN pip install flask[async]
COPY . .
CMD ["flask", "run", "--host", "0.0.0.0"]
#"--host", "0.0.0.0" allows client to make external request to flask app in container