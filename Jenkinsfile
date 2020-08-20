pipeline {
 
    agent any

    options{
        timeout(time: 120, unit: 'SECONDS')
    }

    stages {

        stage('Build') {
            steps {
                //git 'https://github.com/frankhotdocker/project1.git'

                sh "./gradlew clean build"
            }
        }

        stage('ImageBuild') {
            steps {
                sh "./gradlew buildImage"
                //sh "docker build -t docker-registry-default.cloud.scoop-gmbh.de/fheinen/micro:1.0 ."
            }
        }
    }
}

