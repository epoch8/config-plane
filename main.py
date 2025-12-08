import marimo

__generated_with = "0.18.3"
app = marimo.App()


@app.cell
def _():
    from IPython.lib.pretty import pprint
    return (pprint,)


@app.cell
def _():
    from config_plane import create_memory_config_repo
    return (create_memory_config_repo,)


@app.cell
def _(create_memory_config_repo):
    r = create_memory_config_repo({})
    return (r,)


@app.cell
def _(pprint, r):
    pprint(r)
    return


@app.cell
def _(r):
    r.stage.set("k", {"v": 1})
    return


@app.cell
def _(pprint, r):
    pprint(r)
    return


@app.cell
def _(r):
    r.commit()
    return


@app.cell
def _(pprint, r):
    pprint(r)
    return


@app.cell
def _(r):
    r.set("k", {"v": 2})
    return


@app.cell
def _(pprint, r):
    pprint(r)
    return


@app.cell
def _(r):
    r.commit()
    return


@app.cell
def _(pprint, r):
    pprint(r)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
