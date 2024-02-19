pipeline {
    agent any
    
    tools{
        nodejs 'nodejs'
    }

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
            steps {
                script {
                    withCredentials([string(credentialsId: GITHUB_TOKEN, variable: 'GH_TOKEN')]) {
                        env.GIT_LOCAL_BRANCH='main'
                        sh "npm i -g semantic-release"
                        sh "npm install -g @semantic-release/git"
                        sh "semantic-release"
                    }
                    
                    LATEST_TAG = sh(script: 'git describe --tags --abbrev=0', returnStdout: true).trim()
                }
            }                    
        }

        stage('Build Docker Image') {
            steps {
                script {
                    docker.build("${DOCKER_IMAGE_NAME}:${LATEST_TAG}")
                }
            }
        }

        stage('Push Docker Image') {
            steps {
                script {
                    withDockerRegistry(credentialsId: DOCKERHUB_CREDENTIALS) {
                        docker.image("${DOCKER_IMAGE_NAME}:${LATEST_TAG}").push("${LATEST_TAG}")
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
