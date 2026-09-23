#!/bin/sh
set -eu

ENGINE="${ENGINE:-}"
CONTAINER_ENGINE="${CONTAINER_ENGINE:-}"

load_env_file() {
    env_file="${1:-.env}"
    if [ -f "$env_file" ]; then
        echo "Loading variables from $env_file..."
        # Parse non-empty, non-comment lines and export them
        while IFS= read -r line || [ -n "$line" ]; do
            # Skip empty lines and comments starting with #
            case "$line" in
                ''|\#*) continue ;;
            esac
            # Strip potential 'export ' prefix and leading whitespace
            clean_line=$(echo "$line" | sed 's/^[[:space:]]*export[[:space:]]*//')
            
            # Export key=value pairs into current shell context
            key=$(echo "$clean_line" | cut -d'=' -f1)
            val=$(echo "$clean_line" | cut -d'=' -f2-)
            
            # Remove surrounding quotes if present
            val=$(echo "$val" | sed 's/^["'\''\\]*//;s/["'\''\\]*$//')
            
            # Set variable if not already set by explicit environment
            eval ": \${$key:=\"\$val\"}"
            eval "export $key"
        done < "$env_file"
    fi
}
# ------------------------------------------------------------------------------
# 1. Resolve Container Engine (Explicit Env Var or Auto-Detect)
# ------------------------------------------------------------------------------
resolve_engine() {
    # Accept explicit choice via standard variable names
    TARGET="${CONTAINER_ENGINE:-${ENGINE:-}}"

    if [ -n "$TARGET" ]; then
        case "$TARGET" in
            podman|docker)
                if command -v "$TARGET" >/dev/null 2>&1; then
                    ENGINE="$TARGET"
                fi
                ;;
            *)

                ;;
        esac
    else
        # Fallback to auto-detection
        if command -v podman >/dev/null 2>&1; then
            ENGINE="podman"
        elif command -v docker >/dev/null 2>&1; then
            ENGINE="docker"
        fi
    fi

    echo "Using container engine: $ENGINE"
}

# ------------------------------------------------------------------------------
# 2. Helper Functions for Secret Management
# ------------------------------------------------------------------------------
secret_exists() {
    name="$1"
    $ENGINE secret inspect "$name" >/dev/null 2>&1
}

save_secret() {
    name="$1"
    value="$2"

    if secret_exists "$name"; then
        echo "  [!] Secret '$name' already exists. Removing old version..."
        $ENGINE secret rm "$name" >/dev/null
    fi

    # Create secret via stdin to prevent secrets leaking in process table (ps aux)
    printf '%s' "$value" | $ENGINE secret create "$name" - >/dev/null
    echo "  [✓] Secret '$name' saved successfully."
}

prompt_input() {
    var_name="$1"
    prompt_text="$2"
    is_sensitive="${3:-false}"

    while true; do
        if [ "$is_sensitive" = "true" ]; then
            # Turn off terminal echo for passwords/sensitive values
            stty -echo 2>/dev/null || true
            printf "%s: " "$prompt_text"
            read -r val
            stty echo 2>/dev/null || true
            echo ""
        else
            printf "%s: " "$prompt_text"
            read -r val
        fi

        if [ -n "$val" ]; then
            eval "$var_name=\"\$val\""
            break
        else
            echo "  Error: Value cannot be empty. Please try again."
        fi
    done
}

# ------------------------------------------------------------------------------
# 3. Interactive Wizard Flow
# ------------------------------------------------------------------------------
main() {
    load_env_file ".env"
    resolve_engine
    echo "========================================="
    echo "    Container Secrets Setup Wizard       "
    echo "========================================="
    echo ""

    # Prompt user for inputs
    prompt_input DB_USER "Introduce el usuario de base de datos" "false"
    prompt_input DB_PASS "Introduce la contraseña de base de datos" "true"

    echo ""
    echo "Storing secrets in $ENGINE..."
    echo "-----------------------------------------"

    # Store inputs as discrete secrets
    save_secret "database_user" "$DB_USER"
    save_secret "database_password" "$DB_PASS"

    echo ""
    echo "All secrets configured! Listed below:"
    $ENGINE secret ls
}

main "$@"