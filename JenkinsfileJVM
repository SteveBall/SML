pipeline {

    options {
            disableConcurrentBuilds()
            timestamps()
        }

    agent any

        tools {
                 maven 'Maven 3.3.9'
                 jdk 'jdk8'
              }

    stages {
            stage ('Initialise') {
                steps {
                    sh '''
                        echo "Initialise stage"
                        '''
                }
            }

            stage('Test') {
                steps {
                    sh 'mvn test'
                }
                post {
                    always {
                        junit 'target/surefire-reports/*.xml'
                    }
                }
            }

            stage('Package') {
                steps {
                    sh 'mvn -B -DskipTests clean package'
                }
            }

            stage('Deploy') {
                steps {
                    sh 'echo =========================================================================================='
                    sh 'echo DEPLOY!!!!!!!!!'
                    sh 'echo =========================================================================================='
                }
            }

    }
}
