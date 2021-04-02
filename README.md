# backend-metrobus-data
backend-metrobus-query data like units, station, delegations


There are the following endpoints:

http://127.0.0.1:5000/AllStations

-return all available stations in CDMX


http://127.0.0.1:5000/alcaldias
-return the whole available delegation where the stations are placed


http://127.0.0.1:5000/unitsByAlcaldia/delegation

-gets all units/trips that went by a known delegation,you have to write a known delegation instead of 'delegation' at the endpoint, por example:
http://127.0.0.1:5000/unitsByAlcaldia/Tlalpan       - you are looking for trips in Tlalpan


http://127.0.0.1:5000/unidades
-returns all the units in metrobus cdmx



http://127.0.0.1:5000/historialmovimientos/id

gets the movements by a given id, replace id for an integer, it represents the id of every vehicle


the file app.tar contains the docker container, or:


you can build the docker.use cmd to place to the repository folder that contains docker-compose.yml and run the next commands to create the docker.compose:

docker-compose build      ..... build docker compose

docker-compose up          ......run the docker compose


after that, you can use the container to make request by given endpoints.






