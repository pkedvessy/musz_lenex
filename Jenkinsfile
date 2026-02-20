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
    }
}
