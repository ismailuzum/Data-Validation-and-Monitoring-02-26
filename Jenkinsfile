// ============================================================================
// Data Quality Pipeline — Jenkinsfile
// ============================================================================
// Declarative Pipeline syntax for Jenkins.
// Validates the Amazon Sales dataset using Great Expectations + Pydantic
// and sends Slack notifications on validation results.
//
// Prerequisites:
//   - Python 3 installed on the Jenkins agent
//   - Jenkins Credential 'slack-webhook-url' (Secret Text) configured
//     Go to: Manage Jenkins → Credentials → Add → Secret Text
// ============================================================================

pipeline {
    // Run on any available Jenkins agent that has Python installed.
    agent any

    // ── Triggers ────────────────────────────────────────────────────────
    // Poll SCM every 15 minutes for new commits + daily scheduled run at 08:00
    triggers {
        pollSCM('H/15 * * * *')        // Check Git repo for changes every 15 min
        cron('0 8 * * *')              // Daily run at 08:00 (automatic quality check)
    }

    // ── Environment Variables ───────────────────────────────────────────
    // SLACK_WEBHOOK_URL is stored securely in Jenkins Credentials.
    // Jenkins injects it as an environment variable at runtime.
    environment {
        SLACK_WEBHOOK_URL = credentials('slack-webhook-url')
    }

    // ── Pipeline Stages ─────────────────────────────────────────────────
    stages {

        // Stage 1: Install all Python dependencies from requirements.txt.
        stage('Setup') {
            steps {
                sh '''
                    python3 --version
                    pip3 install --upgrade pip
                    pip3 install -r requirements.txt
                '''
            }
        }

        // Stage 2: Run Ruff linter to catch code quality issues early.
        // Errors are logged but do not block the pipeline (|| true).
        stage('Lint') {
            steps {
                sh '''
                    pip3 install ruff
                    ruff check src/ dq_pipeline.py || true
                '''
            }
        }

        // Stage 3: Execute the main data quality pipeline.
        // Runs GE validation → Pydantic validation → Slack notification.
        // Output is saved to validation_output.txt for archiving.
        stage('Data Quality Validation') {
            steps {
                sh 'python3 dq_pipeline.py 2>&1 | tee validation_output.txt'
            }
        }
    }

    // ── Post Actions ────────────────────────────────────────────────────
    // These run AFTER all stages complete, regardless of success or failure.
    post {
        // Always archive the validation report so it can be downloaded
        // from the Jenkins build page (similar to GitHub Artifacts).
        always {
            archiveArtifacts artifacts: 'validation_output.txt',
                             allowEmptyArchive: true,
                             fingerprint: true
        }

        // Log a success message when all validations pass.
        success {
            echo '✅ All validations passed!'
        }

        // Log a failure message and optionally send an extra Slack alert
        // using the Jenkins Slack plugin (must be installed separately).
        failure {
            echo '❌ Validation issues found!'
            // Uncomment the line below if Jenkins Slack plugin is installed:
            // slackSend channel: '#data-quality', color: 'danger', message: "❌ Data Quality Pipeline FAILED — Build #${env.BUILD_NUMBER}"
        }
    }
}
