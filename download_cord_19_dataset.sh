#!/bin/bash

# Downloads 12/12/2020 dataset version

wget https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/historical_releases/cord-19_2020-12-12.tar.gz
tar -xf cord-19_2020-12-12.tar.gz
rm cord-19_2020-12-12.tar.gz
mv 2020-12-12/ cord_19_dataset/
tar -xf cord_19_dataset/document_parses.tar.gz
rm cord_19_dataset/document_parses.tar.gz
tar -xf cord_19_dataset/cord_19_embeddings.tar.gz
rm cord_19_dataset/cord_19_embeddings.tar.gz