# Introduction

A simple service for generate a thumb from a video

## Deploy

### Production

```bash
sls deploy -s production --param="NOTIFY_API_URL=https://studio-api.vitruveo.xyz"
```

### Quality Assuarance

```bash
sls deploy -s qa --param="NOTIFY_API_URL=https://studio-api.vtru.dev"
```
