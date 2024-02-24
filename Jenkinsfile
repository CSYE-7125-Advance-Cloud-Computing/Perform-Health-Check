pipeline {
    agent any

    environment {
        DOCKERHUB_CREDENTIALS = 'docker-hub-credentials'
        DOCKER_IMAGE_NAME = 'mahithchigurupati/perform-health-check'
        GITHUB_TOKEN = 'github-access-token'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Semantic Release') {
            tools {
                nodejs "nodejs"
            }
            steps {
                script {
                    withCredentials([string(credentialsId: GITHUB_TOKEN, variable: 'GH_TOKEN')]) {
                        env.GIT_LOCAL_BRANCH='main'
                        sh 'npx semantic-release'
                    }

                    LATEST_TAG = sh(script: 'git describe --tags --abbrev=0', returnStdout: true).trim()

                }
            }                    
        }

        stage('Build & Push Docker Image') {
            steps {
                script {
                    withDockerRegistry(credentialsId: DOCKERHUB_CREDENTIALS) {
                        sh "docker buildx ls"
                        sh "docker buildx create --name webapplication"
                        sh "docker buildx use webapplication"
                        sh "docker buildx build --push --platform linux/amd64,linux/arm64 --tag $DOCKER_IMAGE_NAME:$LATEST_TAG --tag $DOCKER_IMAGE_NAME:latest ."
                    }
                }
            }
        }

    }

    post {
        success {
            echo 'Pipeline succeeded!'
        }
        failure {
            echo 'Pipeline failed!'
        }
    }
}
