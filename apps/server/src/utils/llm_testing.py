import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from langchain_openai import AzureChatOpenAI


def get_azure_chat_openai():
    """
    Returns ready-to-use AzureChatOpenAI from Key Vault.
    Supports .invoke(), agents, tools, streaming, etc.
    """
    # 1. Key Vault Configuration (fixed URL)
    key_vault_url = "https://fstodevazureopenai.vault.azure.net/"

    # 2. Authenticate and get secrets
    credential = DefaultAzureCredential()
    kv_client = SecretClient(vault_url=key_vault_url, credential=credential)

    # 3. Retrieve your exact secrets
    subscription_key = kv_client.get_secret("llm-api-key").value
    endpoint = kv_client.get_secret("llm-base-endpoint").value
    deployment = kv_client.get_secret("llm-41").value  # Your GPT-4.1 deployment
    api_version = kv_client.get_secret("llm-41-version").value

    # 4. ✅ Native LangChain AzureChatOpenAI
    llm = AzureChatOpenAI(
        azure_deployment=deployment,
        openai_api_version=api_version,
        azure_endpoint=endpoint,
        api_key=subscription_key,
        streaming=True,
        temperature=0,
    )
    return llm


# Usage example
if __name__ == "__main__":
    llm = get_azure_chat_openai()
    print("✅ LLM ready!")

    # Test with messages
    from langchain_core.messages import HumanMessage

    result = llm.invoke([HumanMessage(content="What's the capital of Canada?")])
    print(result.content)