install_dependencies() {
    # this script is for macOS, better it's easy to change it
    # you can run `commands /my_dir`
    # by default it uses the directory the script run from
    pip install PyYAML

    if brew list kubectl &>/dev/null; then
      echo "kubectl is already installed."
    else
      echo "Installing kubectl..."
      brew install kubectl
    fi

    if brew list helm &>/dev/null; then
      echo "helm is already installed."
    else
      echo "Installing helm..."
      brew install helm
    fi
}

update_helm_vars() {
  if [ -z "$HELM_CACHE_HOME" ]; then
      echo "export HELM_CACHE_HOME=$HELM_BASE_DIR/cache" >> ~/.zshrc
      # echo "export HELM_CACHE_HOME=$HELM_BASE_DIR/cache" >> ~/.bash_profile
      mkdir -p "$HELM_BASE_DIR/cache"
    elif [[ "$HELM_CACHE_HOME" != "$HELM_BASE_DIR/cache" ]]; then
      echo "export HELM_CACHE_HOME=$HELM_BASE_DIR/cache" >> ~/.zshrc
      # echo "export HELM_CACHE_HOME=$HELM_BASE_DIR/cache" >> ~/.bash_profile
      mkdir -p "$HELM_BASE_DIR/cache"
    fi
    if [ -z "$HELM_CONFIG_HOME" ]; then
      echo "export HELM_CONFIG_HOME=$HELM_BASE_DIR/config" >> ~/.zshrc
      # echo "export HELM_CONFIG_HOME=$HELM_BASE_DIR/config" >> ~/.bash_profile
      mkdir -p "$HELM_BASE_DIR/config"
    elif [[ "$HELM_CONFIG_HOME" != "$HELM_BASE_DIR/config" ]]; then
      echo "export HELM_CONFIG_HOME=$HELM_BASE_DIR/config" >> ~/.zshrc
      # echo "export HELM_CONFIG_HOME=$HELM_BASE_DIR/config" >> ~/.bash_profile
      mkdir -p "$HELM_BASE_DIR/config"
    fi
    if [ -z "$HELM_DATA_HOME" ]; then
      echo "export HELM_DATA_HOME=$HELM_BASE_DIR/data" >> ~/.zshrc
      mkdir -p "$HELM_BASE_DIR/data"
      # echo "export HELM_DATA_HOME=$HELM_BASE_DIR/data" >> ~/.bash_profile
    elif [[ "$HELM_DATA_HOME" != "$HELM_BASE_DIR/data" ]]; then
        echo "export HELM_DATA_HOME=$HELM_BASE_DIR/data" >> ~/.zshrc
        mkdir -p "$HELM_BASE_DIR/data"
        # echo "export HELM_DATA_HOME=$HELM_BASE_DIR/data" >> ~/.bash_profile
    fi
    if [ -z "$HELM_PLUGINS" ]; then
      echo "export HELM_PLUGINS=$HELM_BASE_DIR/plugins" >> ~/.zshrc
      # echo "export HELM_PLUGINS=$HELM_BASE_DIR/plugins" >> ~/.bash_profile
      mkdir -p "$HELM_BASE_DIR/plugins"
    elif [[ "$HELM_PLUGINS" != "$HELM_BASE_DIR/plugins" ]]; then
      echo "export HELM_PLUGINS=$HELM_BASE_DIR/plugins" >> ~/.zshrc
      # echo "export HELM_PLUGINS=$HELM_BASE_DIR/plugins" >> ~/.bash_profile
      mkdir -p "$HELM_BASE_DIR/plugins"
    fi

    source ~/.zshrc
}

build_image() {
    CURRENT_DIR=$(pwd)
    cd "$HELM_BASE_DIR" || true
    cd "../"
    sh build_images.sh "$TAG"
    cd "$CURRENT_DIR" || true
}