from start_files.app import create_app

app = create_app()

#if __name__ == "__main__":
#    import uvicorn
#    uvicorn.run(app, host="127.0.0.1", port=5000)
#    gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:5000
#   netstat -tuln | grep LISTEN
#   lsof -i :5000
#   kill -9 282229
#nohup uvicorn main:app --host 0.0.0.0 --port 5000 --reload > uvicorn.log 2>&1 &