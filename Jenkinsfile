pipeline {
    agent any

    options {
        skipDefaultCheckout(true)
    }

    environment {
        DB_HOST = "10.0.0.61"
        DB_PORT = "5432"
        DB_NAME = "lenex"
        DB_SCHEMA = "public"
        IMAGE = "musz-lenex"
        LENEX_DIR = "/lenex_files"
    }

    stages {

        stage('Prepare workspace') {
            steps {
                sh '''
                    JENKINS_UID=$(id -u)
                    JENKINS_GID=$(id -g)
                    docker run --rm -v "$(pwd)":/ws alpine \
                        sh -c "chown -R ${JENKINS_UID}:${JENKINS_GID} /ws/lenex_files 2>/dev/null; rm -rf /ws/scripts/lenex_files 2>/dev/null; true"
                '''
                checkout scm
            }
        }

        stage('Build image') {
            steps {
                sh "docker build -t ${IMAGE} ."
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

        stage('Fetch LENEX') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'postgres-lenex-db', usernameVariable: 'DB_USER', passwordVariable: 'DB_PASSWORD')]) {
                    sh """
                    mkdir -p lenex_files
                    docker run --rm -i --user \$(id -u):\$(id -g) \
                        -v \$(pwd)/scripts:/scripts \
                        -v \$(pwd)/lenex_files:${LENEX_DIR} \
                        -e DB_HOST=${DB_HOST} -e DB_PORT=${DB_PORT} \
                        -e DB_NAME=${DB_NAME} -e DB_USER=\$DB_USER \
                        -e DB_PASSWORD=\$DB_PASSWORD \
                        -e LENEX_DIR=${LENEX_DIR} \
                        ${IMAGE} \
                        python -u /scripts/fetch_lenex.py
                    """
                }
            }
        }

        stage('Backup LENEX to Google Drive') {
            steps {
                withCredentials([
                    usernamePassword(credentialsId: 'postgres-lenex-db', usernameVariable: 'DB_USER', passwordVariable: 'DB_PASSWORD'),
                    file(credentialsId: 'gdrive-oauth-token', variable: 'GDRIVE_TOKEN')
                ]) {
                    sh """
                    docker run --rm -i --user \$(id -u):\$(id -g) \
                        -v \$(pwd)/scripts:/scripts \
                        -v \$(pwd)/lenex_files:${LENEX_DIR} \
                        -v \$GDRIVE_TOKEN:/secrets/token.json \
                        -e DB_HOST=${DB_HOST} -e DB_PORT=${DB_PORT} \
                        -e DB_NAME=${DB_NAME} -e DB_USER=\$DB_USER \
                        -e DB_PASSWORD=\$DB_PASSWORD \
                        -e LENEX_DIR=${LENEX_DIR} \
                        -e GDRIVE_FOLDER_ID=YOUR_FOLDER_ID \
                        ${IMAGE} \
                        python -u /scripts/backup_to_gdrive.py
                    """
                }
            }
        }

        stage('Import LENEX') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'postgres-lenex-db', usernameVariable: 'DB_USER', passwordVariable: 'DB_PASSWORD')]) {
                    sh """
                    docker run --rm -i --user \$(id -u):\$(id -g) \
                        -v \$(pwd)/scripts:/scripts \
                        -v \$(pwd)/lenex_files:${LENEX_DIR} \
                        -e DB_HOST=${DB_HOST} -e DB_PORT=${DB_PORT} \
                        -e DB_NAME=${DB_NAME} -e DB_USER=\$DB_USER \
                        -e DB_PASSWORD=\$DB_PASSWORD \
                        -e LENEX_DIR=${LENEX_DIR} \
                        ${IMAGE} \
                        python -u /scripts/import_lenex.py
                    """
                }
            }
        }

        stage('Scrape MUSZ') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'postgres-lenex-db', usernameVariable: 'DB_USER', passwordVariable: 'DB_PASSWORD')]) {
                    sh """
                    docker run --rm -i --user \$(id -u):\$(id -g) \
                        -v \$(pwd)/scripts:/scripts \
                        -e DB_HOST=${DB_HOST} -e DB_PORT=${DB_PORT} \
                        -e DB_NAME=${DB_NAME} -e DB_USER=\$DB_USER \
                        -e DB_PASSWORD=\$DB_PASSWORD \
                        ${IMAGE} \
                        python -u /scripts/scrape_musz_result_pages.py
                    """
                }
            }
        }
    }
}
