from fastapi import APIRouter
from endpoints import auth, user, sam, validate, project, module, prospects, upload

router = APIRouter()
router.include_router(auth.router)
router.include_router(user.router)
router.include_router(sam.router)
router.include_router(validate.router)
router.include_router(project.router)
router.include_router(module.router)
router.include_router(prospects.router)
router.include_router(upload.router)
