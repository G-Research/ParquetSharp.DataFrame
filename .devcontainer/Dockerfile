FROM mcr.microsoft.com/devcontainers/dotnet:1-9.0-bookworm

# Install additional development tools
RUN apt-get update && apt-get install -y \
    git \
    procps \
    && rm -rf /var/lib/apt/lists/*

# [Optional] Install global dotnet tools
RUN dotnet tool install -g dotnet-format 