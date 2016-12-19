FROM node:7.2

COPY . /app

RUN npm install

ENTRYPOINT ["/app/bin/amqp-to-mongo"]

CMD [""]