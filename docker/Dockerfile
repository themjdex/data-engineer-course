FROM postgres:15

ENV POSTGRES_USER=themjdex
ENV POSTGRES_PASSWORD=12345
ENV POSTGRES_DB=de_course

COPY /app/create.sql /docker-entrypoint-initdb.d/

VOLUME /test:/var/lib/postgresql/data

CMD ["postgres"]