from celery import Celery, chain, group

app = Celery('tasks', backend='rpc://', broker='pyamqp://')


@app.task
def add(x, y):
    return x + y


@app.task
def prep():
    nt = "NIOSS_DATA"
    return nt


@app.task
def single(a, b="B", c="C", nt="NO_NIOSS"):
    res=f"{a} {b=} {c=} {nt=}"
    return res


@app.task
def post(res):
    print("final", res)
    return res


@app.task
def func():
    it = [(n, f"b{n}", f"c{n}") for n in range(10)]
    pipe = chain(prep.s(), group(single(a, b, c) for (a, b, c) in it), post.s())
    return pipe
