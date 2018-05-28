This is a test Ratpack app that:

./gradlew build buildImage

docker tag ... micro
docker run -p 5050:5050 --rm -v micro-data --name micro micro
