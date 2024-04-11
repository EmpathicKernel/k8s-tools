###############################
# This script is used to easily scale deployments up or down 
#
# Options:
#   -n, --namespace             Specify namespace to run against (Required)
#   --loglevel                  Log level to output. Defaults to "info". Options are: debug, info, warning, error
#   --timeout                   Timeout in seconds for the API calls. Defaults to 60
#   --release-label             Deployment label that maps to the Helm release name. Defaults to "release"
#   --scale-in                  Flag to determine if deployments should be scaled in. Will scale deployments to 
#                                  replica count from Helm release or replica count passed through --replicas option. If
#                                  no Helm release is found and no replica count is passed in, it will default to 1. Will not
#                                  scale higher than number of current replicas
#   --scale                     Flag to determine if deployments should be scaled out. Will scale deployments to 
#                                   replica count from Helm release or replica count passed through --replicas option. If
#                                  no Helm release is found and no replica count is passed in, it will default to 1. Will not
#                                  scale lower than number of current replicas
#   --replicas                  Number of replicas to scale to. Defaults to 1
#   --no-helm-replica-check     Flag to skip the check of Helm to find replica count. Requires --replicas to be defined
#   --namespace-wide            Flag to determine if full namespace should be scaled. If not set, deployments must be passed in
#                                   unless no scaling is occurring
#   
# Arguments:
#   Specific deployment names can be passed as additional arguments. Will override --namespace-wide if used in conjunction
# 
# Usage:
#   
#       Scale out the number of replicas for entire namespace "default" to 4
#           python3 ./k8s_scaling.py -n default --scale --namespace-wide --replicas 4
#
#       Scale in the number of replicas for example-deployment within namespace "default" to 0
#           python3 ./k8s_scaling.py -n default --scale-in --replicas 0 example-deployment
#
#       Get a printout of current number of replicas within namespace "test-namespace"
#           python3 ./k8s_scaling.py -n test-namespace
# 

import sys
import logging
import optparse
import asyncio
from pyhelm3 import Client
from kubernetes import client, config
from kubernetes.client.rest import ApiException


parser = optparse.OptionParser()
parser.add_option("-n", "--namespace", dest = "namespace", action = "store", help = "Specify namespace to run against")
parser.add_option("--loglevel", dest = "loglevel", action = "store", default = "INFO", help = "Log level to output")
parser.add_option("--timeout", dest = "timeout", action = "store", default = 60, help = "Timeout for Kubernete API calls")
parser.add_option("--release-label", dest = "releaseLabel", action = "store", default = "release", help = "Label that maps to helm release name")
parser.add_option("--scale-in", dest = "scaleIn", action = "store_true", help = "Flag to determine if deployments should be scaled in. Cannot be used with --scale")
parser.add_option("--scale", dest = "scaleOut", action = "store_true", help = "Flag to determine if deployments should be scaled out. Cannot be used with --scale-in")
parser.add_option("--replicas", dest = "replicas", action = "store", help = "Number of replicas to scale to. Does not overwrite Helm replica count. Must be used with either --scale or --scale-in")
parser.add_option("--no-helm-replica-check", dest = "helmCheck", action = "store_false", default = True, help = "Skip checking Helm charts for replica counts. If used, --replicas must be specified.")
parser.add_option("--namespace-wide", dest = "namespaceWide", action = "store_true", default = False, help = "Scale all deployments within the namespace. Overridden by deployments passed in as arguments")

options, args = parser.parse_args()

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=options.loglevel.upper())

selDeployments = []

def optionError(message):
    logging.error(f"{message}... Exiting")
    sys.exit()

if options.namespace:
    Namespace = options.namespace
else:
    optionError("Namespace not supplied")

if options.scaleIn and options.scaleOut:
    optionError("You cannot specify both a scale-in and scale-out event")
else:
    ScaleIn = options.scaleIn
    ScaleOut = options.scaleOut

if not options.helmCheck and not options.replicas:
    optionError("Replicas must be specified when not running a Helm check")

print(not (ScaleIn or ScaleOut))
if not options.namespaceWide and not args and (ScaleIn or ScaleOut):
    optionError("Deployments must be passed in as arguments unless --namespace-wide is used or no scaling is occurring")

DefaultReplicas = 1
UserDefaultedReplicas = False
if options.replicas:
    DefaultReplicas = int(options.replicas)
    UserDefaultedReplicas = True

if args:
    logging.info(f"Only looking for these deployments: {args}")
    selDeployments = args


Timeout = options.timeout
GlobalReleaseLabel = options.releaseLabel
HelmCheck = options.helmCheck




def listDeploymentInfo(deployList):
    deployInfo = []

    try:
        api_response = apps_v1.list_namespaced_deployment(Namespace, limit=60, timeout_seconds=Timeout)

        logging.debug(f"API Response for Namespace Lookup: {api_response}")

    except ApiException as e:
        print("Exception when calling AppsV1Api->list_namespaced_deployment: %s\n" % e)
    
    if deployList:
        for item in api_response.items:
            if item.metadata.name in deployList:
                logging.debug(f"Deployment data for {item.metadata.name}:\n{item}")
                deployInfo.append(item)
        return deployInfo

    deployInfo = api_response.items
    return deployInfo


def findReleaseNames(deployments):

    deployedReleases = {}

    for item in deployments:
        deployment_name = item.metadata.name
        release = item.metadata.labels.get(GlobalReleaseLabel, None)
        logging.debug(f"Deployment {deployment_name} has label {GlobalReleaseLabel} of {release}")
        deployedReleases[deployment_name] = {"helmRelease" : release}

    return deployedReleases


def findCurrentReplicas(deployments):

    currentDeployments = {}

    for item in deployments:
        deployment_name = item.metadata.name
        curReplicas = item.spec.replicas
        logging.debug(f"Deployment {deployment_name} has replica count {curReplicas}")
        currentDeployments[deployment_name] = {"curReplicas" : curReplicas}

    return currentDeployments


def helmReplicaCount(helmRelease, deploymentName):

    minReplicas = -1

    rels = asyncio.run(helm_client.get_current_revision(helmRelease, namespace = Namespace))

    logging.debug(f"Full current revision:\n{rels}")

    for rel in rels.resources_:
        logging.debug(f"Parsing {helmRelease} current revision:\n{rel}")
        if rel is None:
            break
        if rel["kind"] == "HorizontalPodAutoscaler" and rel["spec"]["scaleTargetRef"]["name"] == deploymentName:
            minReplicas = rel["spec"]["minReplicas"]
            logging.info(f"HorizontalPodAutoscaler found for {helmRelease} targeting {deploymentName}")
            
    
    if minReplicas < 0:
        logging.warning(f"Could not find minReplicas within HPA spec for {deploymentName}. This could mean this deployment does not have HPA configured.")

    logging.info(f"Replica count for {helmRelease} is {minReplicas}")

    return minReplicas


def buildDeploymentData():

    deploys = listDeploymentInfo(selDeployments)

    deploymentData = {}

    deploymentData = findCurrentReplicas(deploys)

    if HelmCheck:
        deploymentReleaseData = findReleaseNames(deploys)
        print(deploymentReleaseData)
        for item in deploymentReleaseData:
                deploymentData[item].update(deploymentReleaseData[item])
        logging.debug(f"Deployment data Dictionary: {deploymentData}")
        logging.info("Finding replica counts for releases.")
        for release in deploymentData:
            newReplicas = DefaultReplicas
            if deploymentData[release]["helmRelease"] is None:
                logging.warning(f"No helmRelease found for {release}")
            else:
                deploymentData[release].update({"helmReplicas" : helmReplicaCount(deploymentData[release]["helmRelease"], release)})
            deploymentData[release].update({"newReplicas": newReplicas})

        logging.debug(f"Releases Dict with replicas: {deploymentData}")
    else:
        for item in deploymentData:
            deployment_name = item
            logging.debug(f"Deployment {deployment_name} set for {DefaultReplicas} replicas")
            deploymentData[deployment_name].update({"newReplicas" : DefaultReplicas})

    return deploymentData


def scaleDeployment(depName, depDetails):

    depReplicas = -1

    if depDetails.get("helmReplicas"):
        if UserDefaultedReplicas and (depDetails["helmReplicas"] != DefaultReplicas):
            logging.warning(f"Helm replicas do not match user specified replicas. Continuing to scale based on user defined replica count...")
            depReplicas = DefaultReplicas
        else:
            depReplicas = int(depDetails["helmReplicas"])
            if depReplicas < 0:
                depReplicas = DefaultReplicas
                logging.info(f"Helm replica value was not found for {depName}... Defaulting to {depReplicas}")

    if depDetails.get("newReplicas"):
        depReplicas = int(depDetails["newReplicas"])
        logging.info(f'Setting replica value for {depName} to {depDetails["newReplicas"]}')

    if depReplicas < 0:
        depReplicas = DefaultReplicas
        logging.warning(f"Something went wrong when setting replicas. Defaulting to {DefaultReplicas}")

    logging.info(f"Scaling {depName} to {depReplicas}")

    if ScaleOut and depReplicas <= depDetails["curReplicas"]:
        logging.warning(f'Not scaling as current replica count of {depDetails["curReplicas"]} is higher or equal than desired replica count of {depReplicas}')
        return
    if ScaleIn and depReplicas >= depDetails["curReplicas"]:
        logging.warning(f'Not scaling as current replica count of {depDetails["curReplicas"]} is lower or equal than desired replica count of {depReplicas}')
        return
    
    try:
        api_response = apps_v1.patch_namespaced_deployment_scale(depName, Namespace, {'spec': {'replicas': depReplicas}})

        logging.debug(f"API Response for Deployment Scale Patch: {api_response}")

    except ApiException as e:
        print(f"Exception when calling AppsV1Api->patch_namespaced_deployment_scale: {e}\n")

    
def main():

    if not ScaleIn and not ScaleOut:
        logging.info("No scaling specified. Will perform a dry-run.")

    deployments = buildDeploymentData()

    logging.debug(f"Replica data:\n{deployments}")

    if ScaleIn or ScaleOut:
        answer = input(f"Replica Data:\n{deployments}\n\nContinue with scaling? [(Y)es/(N)o]:  ")
        if answer.lower() in ["y","yes"]:
            for deployment in deployments:
                scaleDeployment(deployment, deployments[deployment])
        else:
            logging.info(f"Scaling aborted...")
    else:
        logging.info(f"Current replica data:\n{deployments}")


if __name__ == "__main__":
    config.load_kube_config()
    apps_v1 = client.AppsV1Api()

    helm_client = Client()

    main()