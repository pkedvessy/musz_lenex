pipeline {
    agent any

    environment {
        DB_HOST = "10.0.0.61"
        DB_PORT = "5432"
        DB_NAME = "lenex"
        DB_SCHEMA = "public"
    }

    stages {

        stage('Checkout') {
            steps {
                git branch: 'main',
                    url: 'https://github.com/pkedvessy/musz_lenex.git'
            }
        }

        stage('Flyway Migrate') {
            steps {

                withCredentials([usernamePassword(
                    credentialsId: 'postgres-lenex-db',
                    usernameVariable: 'DB_USER',
                    passwordVariable: 'DB_PASSWORD'
                )]) {

                    sh """
                    docker run --rm \
                      -v \$(pwd)/sql:/flyway/sql \
                      flyway/flyway \
                      -url=jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME} \
                      -user=\$DB_USER \
                      -password=\$DB_PASSWORD \
                      -schemas=${DB_SCHEMA} \
                      migrate
                    """
                }
            }
        }

        stage('Fetch & Import LENEX') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'postgres-lenex-db', usernameVariable: 'DB_USER', passwordVariable: 'DB_PASSWORD')]) {
                    sh """
                    docker run --rm -v \$(pwd)/scripts:/scripts \
                        -e DB_HOST=${DB_HOST} -e DB_PORT=${DB_PORT} \
                        -e DB_NAME=${DB_NAME} -e DB_USER=$DB_USER \
                        -e DB_PASSWORD=$DB_PASSWORD \
                        python:3.12-slim \
                        bash -c "pip install psycopg2-binary requests beautifulsoup4 && python /scripts/fetch_lenex.py"
                    """
                }
            }
        }
    }
}
