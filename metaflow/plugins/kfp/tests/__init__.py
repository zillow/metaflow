from .... import R


def _python():
    if R.use_r():
        return "python3"
    else:
        return "python"
