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
                sh "echo './gradlew buildImage'"
            }
        }
    }
}

