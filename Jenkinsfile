pipeline {
 
    agent any

    options{
        timeout(time: 60, unit: 'SECONDS')
    }

    stages {
        stage('Build') {
            steps {
                // Get some code from a GitHub repository
                // git 'https://github.com/frankhotdocker/project1.git'

                // Run Maven on a Unix agent.
                sh "./gradlew clean build"

            }
        }
    }
}

