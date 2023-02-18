import boto3
import botocore
# import jsonschema
import json
import traceback

from requests.utils import quote
from botocore.exceptions import ClientError

from extutil import remove_none_attributes, account_context, ExtensionHandler, \
    ext, component_safe_name, handle_common_errors, random_id

eh = ExtensionHandler()

sns = boto3.client("sns") ### View top of AWS page
def lambda_handler(event, context):
    try:
        print(f"event = {event}")
        account_number = account_context(context)['number']
        region = account_context(context)['region']
        eh.capture_event(event)
        prev_state = event.get("prev_state") or {}
        cdef = event.get("component_def") 
        project_code = event.get("project_code")
        repo_id = event.get("repo_id")
        cname = event.get("component_name")

        #Above is Boilerplate besides the name of the client
        
        topic_name = cdef.get("name") or component_safe_name(project_code, repo_id, cname, max_chars=250)
        
        # Naming is rigid, always call component_safe_name with the correct max_chars
        # If its not clear what max_chars should be, remove the argument from the function

        tags = cdef.get("tags") or {}
        data_protection_policy = cdef.get("data_protection_policy")

        delivery_policy = cdef.get("delivery_policy") 
        display_name = cdef.get("display_name")
        fifo_topic = cdef.get("fifo_topic") or False
        if (fifo_topic == True) and (not topic_name.endswith(".fifo")):
            topic_name += ".fifo"

        policy = cdef.get("policy")
        signature_version = cdef.get("signature_version") or 1
        tracing_config = cdef.get("tracing_config") or "PassThrough"

        if tracing_config == "Active" and fifo_topic == True:
            eh.declare_return(200, 0, error_code="Fifo Topics do not support Active Tracing")
            return eh.finish()

        kms_master_key_id = cdef.get("kms_master_key_id")

        content_based_deduplication = cdef.get("content_based_deduplication") or False

        attributes = remove_none_attributes({
            "ContentBasedDeduplication": content_based_deduplication,
            "FifoTopic": fifo_topic,
            "KmsMasterKeyId": kms_master_key_id,
            "DisplayName": display_name,
            "Policy": policy,
            "DeliveryPolicy": delivery_policy,
            "SignatureVersion": signature_version,
            "TracingConfig": tracing_config
        })

        topic_arguments = remove_none_attributes({
            "Name": topic_name,
            "Attributes": attributes,
            "Tags": format_tags(tags),
            "DataProtectionPolicy": data_protection_policy
        })
        # Assemble Create Topic (or other thing) Arguments

        #Pass back data is boilerplate
        pass_back_data = event.get("pass_back_data", {})
        if pass_back_data:
            pass

        #Declare what functions should be called for the two different operations
        elif event.get("op") == "upsert":
            eh.add_op("get_topic")

        elif event.get("op") == "delete":
            eh.add_op("delete_topic", {"name":topic_name, "only_delete": True})

        get_topic(topic_arguments, account_number, region)
        create_topic(topic_arguments, account_number, region)
        update_topic(topic_arguments, account_number, region)
        delete_topic()
            
        return eh.finish()

    except Exception as e:
        msg = traceback.format_exc()
        print(msg)
        eh.add_log("Unexpected Error", {"error": msg}, is_error=True)
        eh.declare_return(200, 0, error_code=str(e))
        return eh.finish()


@ext(handler=eh, op="get_topic")
def get_topic(topic_arguments, account_number, region):

    topic_arn = get_topic_arn_from_name(topic_arguments['Name'], account_number, region)

    try:
        response = sns.get_topic_attributes(
            TopicArn=topic_arn
        )

        # If the topic exists, we need to check if the attributes are the same

    except ClientError as e:
        if e.response["Error"]["Code"] == "NotFoundException":
            eh.add_op("create_topic")
        else:
            handle_common_errors(e, eh, "Get Topic Failure", 0)


def format_tags(tags_dict):
    return [{"Key": k, "Value": v} for k,v in tags_dict]

# def unformat_tags(tags_list):
#     return {t["Key"]: t["Value"] for t in tags_list}

def get_topic_arn_from_name(name, account_number, region):
    return f"arn:aws:sns:{region}:{account_number}:{name}"