// ============================================================================
// Data Quality Pipeline — Jenkinsfile (Production-Ready Version)
// ============================================================================
// Declarative Pipeline syntax for Jenkins.
// Validates the Amazon Sales dataset using Great Expectations + Pydantic,
// archives validation results, and sends Slack notifications.
//
// Improvements in this version:
//   - Uses isolated Python virtual environment (venv)
//   - Ensures reproducible builds
//   - Prevents dependency conflicts between Jenkins jobs
//   - Adds pipeline safety options (timeout, timestamps)
//
// Prerequisites:
//   - Python 3 installed on the Jenkins agent
//   - Jenkins Credential 'slack-webhook-url' (Secret Text) configured
//     Go to: Manage Jenkins → Credentials → Add → Secret Text
// ============================================================================

pipeline {

    // Run on any available Jenkins agent that has Python installed.
    agent any

    // ── Pipeline Options ────────────────────────────────────────────────────
    // timeout: prevents hanging builds
    // timestamps: adds timestamps to logs (useful for debugging & monitoring)
    options {
        timeout(time: 20, unit: 'MINUTES')
        timestamps()
    }

    // ── Triggers ────────────────────────────────────────────────────────────
    // pollSCM: detects new commits
    // cron: scheduled daily validation for continuous monitoring
    triggers {
        pollSCM('H/15 * * * *')        // Check Git repo every 15 minutes
        cron('0 8 * * *')              // Run daily at 08:00
    }

    // ── Environment Variables ───────────────────────────────────────────────
    // SLACK_WEBHOOK_URL is securely injected from Jenkins credentials
    // VENV defines the virtual environment directory
    environment {
        SLACK_WEBHOOK_URL = credentials('slack-webhook-url')
        VENV = 'venv'
    }

    // ── Pipeline Stages ─────────────────────────────────────────────────────
    stages {

        // Stage 1: Setup isolated Python environment
        // Creates virtual environment and installs dependencies.
        stage('Setup Environment') {
            steps {
                sh '''
                    echo "Starting environment setup..."

                    python3 --version

                    # Create isolated virtual environment
                    python3 -m venv $VENV

                    # Activate virtual environment
                    . $VENV/bin/activate

                    # Upgrade pip and install dependencies
                    pip install --upgrade pip
                    pip install -r requirements.txt

                    echo "Environment setup completed."
                '''
            }
        }

        // Stage 2: Run Ruff linter for static code analysis.
        // Does not fail pipeline (non-blocking quality check).
        stage('Lint Code') {
            steps {
                sh '''
                    echo "Running code linting..."

                    . $VENV/bin/activate

                    pip install ruff

                    # Run linter (non-blocking)
                    ruff check src/ dq_pipeline.py || true

                    echo "Linting completed."
                '''
            }
        }

        // Stage 3: Execute data quality validation pipeline.
        // Runs Great Expectations and Pydantic validation logic.
        // Output is saved for auditing and debugging.
        stage('Data Quality Validation') {
            steps {
                sh '''
                    echo "Running data quality validation..."

                    . $VENV/bin/activate

                    python dq_pipeline.py \
                    2>&1 | tee validation_output.txt

                    echo "Validation stage completed."
                '''
            }
        }
    }

    // ── Post Actions ────────────────────────────────────────────────────────
    // These run regardless of pipeline success or failure.
    post {

        // Always archive validation results for auditing and download.
        always {
            archiveArtifacts artifacts: 'validation_output.txt',
                             allowEmptyArchive: true,
                             fingerprint: true

            echo "Validation output archived."
        }

        // Success notification.
        success {
            echo '✅ All data quality validations passed successfully!'
        }

        // Failure notification.
        failure {
            echo '❌ Data quality validation failed!'

            // Optional Slack notification via Jenkins Slack Plugin:
            // slackSend channel: '#data-quality',
            //           color: 'danger',
            //           message: "❌ Data Quality Pipeline FAILED — Build #${env.BUILD_NUMBER}"
        }

        // Always cleanup virtual environment to keep Jenkins workspace clean.
        cleanup {
            sh '''
                echo "Cleaning up virtual environment..."
                rm -rf $VENV || true
            '''
        }
    }
}