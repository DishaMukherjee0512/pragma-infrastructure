from azure.common.client_factory import get_client_from_cli_profile
from azure.mgmt.resource import SubscriptionClient

sub_client = get_client_from_cli_profile(SubscriptionClient)
print(sub_client)
for sub in sub_client.subscriptions.list():
    print(sub.subscription_id)