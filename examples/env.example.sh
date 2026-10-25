# SingleStore Database Rebuild Kit: optional environment for an operator terminal.
# Author: John Bergamini
# Nothing in this file is a secret. Passwords come from the Keychain item named in the
# config, or from the variable named by password_env, exported in the terminal and never
# written to a file. Copy to customer/env.sh and source it before running the kit.

# Default config path (the kit reads customer/config.yaml when this is not set).
export S2REBUILD_CONFIG=customer/config.yaml

# Default run id (the kit uses the most recent run when this is not set).
# export S2REBUILD_RUN=20261101-0500

# Runner password when the config says password_env instead of password_keychain.
# The value is typed at the prompt and never echoed:
# read -rs S2REBUILD_RUNNER_PASSWORD && export S2REBUILD_RUNNER_PASSWORD

# Pipeline credentials for O9 (route kit), one JSON per source type, set for O9 only:
# read -rs S2REBUILD_KAFKA_CREDENTIALS && export S2REBUILD_KAFKA_CREDENTIALS
# unset S2REBUILD_KAFKA_CREDENTIALS after O9 has passed.
