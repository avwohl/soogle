def site_url(request):
    return {"site_url": f"{request.scheme}://{request.get_host()}"}
