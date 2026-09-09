cat > Dockerfile << 'EOF'
FROM public.ecr.aws/lambda/python:3.9

# Copy function code
COPY src ${LAMBDA_TASK_ROOT}
# db/ lives outside src/, so it needs its own COPY or the
# `from db.easebase_conn import ...` imports fail at runtime.
COPY db ${LAMBDA_TASK_ROOT}/db

# Install the function's dependencies using file requirements.txt
# from your project folder.
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]
EOF