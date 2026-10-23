"""RFC 9457 problem details: every error the API returns is application/problem+json."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

PROBLEM_BASE = "https://epigcorpus.example/problems/"
MEDIA_TYPE = "application/problem+json"


class Problem(Exception):
    """Raise from a route to answer with a problem document."""

    def __init__(self, status: int, slug: str, title: str, detail: str, **extensions):
        super().__init__(detail)
        self.status = status
        self.slug = slug
        self.title = title
        self.detail = detail
        self.extensions = extensions


def _instance(request: Request) -> str:
    query = request.url.query
    return request.url.path + (f"?{query}" if query else "")


def problem_response(request: Request, problem: Problem) -> JSONResponse:
    body = {
        "type": PROBLEM_BASE + problem.slug,
        "title": problem.title,
        "status": problem.status,
        "detail": problem.detail,
        "instance": _instance(request),
        **problem.extensions,
    }
    return JSONResponse(body, status_code=problem.status, media_type=MEDIA_TYPE)


def not_found(detail: str) -> Problem:
    return Problem(404, "not-found", "Not found", detail)


def invalid(name: str, detail: str, where: str = "query") -> Problem:
    return Problem(
        422,
        "validation",
        "Invalid request",
        "1 parameter is invalid.",
        errors=[{"in": where, "name": name, "detail": detail}],
    )


def install(app: FastAPI) -> None:
    """Route every error, including FastAPI's own validation, through problem+json."""

    @app.exception_handler(Problem)
    async def _problem(request: Request, exc: Problem):
        return problem_response(request, exc)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        errors = []
        for error in exc.errors():
            location = list(error.get("loc", ()))
            where = location[0] if location else "query"
            name = ".".join(str(part) for part in location[1:]) or str(where)
            errors.append({"in": str(where), "name": name, "detail": error.get("msg", "invalid")})
        count = len(errors)
        problem = Problem(
            422,
            "validation",
            "Invalid request",
            f"{count} parameter{'s are' if count != 1 else ' is'} invalid.",
            errors=errors,
        )
        return problem_response(request, problem)

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException):
        titles = {
            404: ("not-found", "Not found"),
            405: ("method-not-allowed", "Method not allowed"),
        }
        slug, title = titles.get(exc.status_code, ("http-error", "HTTP error"))
        return problem_response(request, Problem(exc.status_code, slug, title, str(exc.detail)))
