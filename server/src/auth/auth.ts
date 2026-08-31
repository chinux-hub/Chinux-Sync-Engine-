import { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";

/**
 * Minimal JWT bearer auth. Swap this out for your existing auth system
 * (this engine doesn't dictate identity/auth — it only needs a stable
 * userId per request). Issue tokens however you already do (your own
 * login endpoint, OAuth, etc.) and sign them with JWT_SECRET.
 */
export function authMiddleware(secret: string) {
  return (req: Request, res: Response, next: NextFunction) => {
    const header = req.headers.authorization ?? "";
    const token = header.startsWith("Bearer ") ? header.slice(7) : null;
    if (!token) return res.status(401).json({ error: "missing bearer token" });
    try {
      const payload = jwt.verify(token, secret) as { userId: string };
      (req as any).userId = payload.userId;
      next();
    } catch {
      return res.status(401).json({ error: "invalid token" });
    }
  };
}
