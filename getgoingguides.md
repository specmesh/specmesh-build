# Guides 

## Get going (manually provisioning AK clusters) in 30 seconds (without GitOps)
This guide assumes you have a new git repository and want to manually run the specmesh cli to provision a cluster. We are in the tinkering phase.
1. step one
1. step two

## Getting started with the GitOps workflow
1. Build a repository template for your company (use the [Specmesh Template](https://github.com/specmesh/specmesh-template) as a starting point)
1. New projects are started by creating the repository from the template. The associated specmesh app spec is configured and run through a PR - where PR workflow is configured to provision inrastructure at each stage of the build. i.e. builds are promoted from Dev -> the workflow hooks call against the SpecMesh->CLI->Provision command
1. As the Spec is updated, the PR promoted build changes through each environment
