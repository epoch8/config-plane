import marimo

__generated_with = "0.18.3"
app = marimo.App()


@app.cell
def _():
    import config_plane as cp
    from IPython.lib.pretty import pprint
    return cp, pprint


@app.cell
def _(cp):
    r = cp.ConfigRepo.create()
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
def _():
    return


if __name__ == "__main__":
    app.run()
