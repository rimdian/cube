FROM cubejs/cube:latest

COPY /cube.js .
RUN yarn install
