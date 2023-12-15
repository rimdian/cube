FROM cubejs/cube:latest

COPY /cubejs/cube.js .
RUN yarn install
