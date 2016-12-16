FROM node:7.2

COPY . /app

ENTRYPOINT ["/app/bin/amqp-to-mongo"]

CMD [""]