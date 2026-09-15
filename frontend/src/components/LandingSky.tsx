import { useEffect, useRef } from 'react';

type Star = {
  x: number;
  y: number;
  r: number;
  alpha: number;
  twinkleSpeed: number;
  twinklePhase: number;
  variable: boolean;
};

type Supernova = {
  x: number;
  y: number;
  start: number;
  duration: number;
  maxR: number;
};

type Comet = {
  x: number;
  y: number;
  vx: number;
  vy: number;
  start: number;
  life: number;
};

export default function LandingSky({ className }: { className?: string }) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const rafRef = useRef<number | null>(null);
  const starsRef = useRef<Star[] | null>(null);
  const offscreenRef = useRef<HTMLCanvasElement | null>(null);
  const supernovasRef = useRef<Supernova[]>([]);
  const cometsRef = useRef<Comet[]>([]);
  const timersRef = useRef<number[]>([]);

  useEffect(() => {
    const canvas = canvasRef.current!;
    if (!canvas) return;
    const ctx = canvas.getContext('2d')!;

    let mounted = true;
    const dpr = Math.max(1, window.devicePixelRatio || 1);

    function resize() {
      const rect = canvas.getBoundingClientRect();
      canvas.width = Math.max(1, Math.floor(rect.width * dpr));
      canvas.height = Math.max(1, Math.floor(rect.height * dpr));
      canvas.style.width = `${rect.width}px`;
      canvas.style.height = `${rect.height}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      // rebuild offscreen
      buildOffscreen();
    }

    function prefersReducedMotion() {
      try {
        return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      } catch {
        return false;
      }
    }

    function buildStars(count: number) {
      const rect = canvas.getBoundingClientRect();
      const w = rect.width;
      const h = rect.height;
      const arr: Star[] = [];
      for (let i = 0; i < count; i++) {
        const x = Math.random() * w;
        const y = Math.random() * h;
        const r = Math.random() * 1.4 + 0.3;
        const alpha = 0.2 + Math.random() * 0.8;
        const twinkleSpeed = 0.2 + Math.random() * 0.9;
        const twinklePhase = Math.random() * Math.PI * 2;
        const variable = Math.random() < 0.06; // 6% variable
        arr.push({ x, y, r, alpha, twinkleSpeed, twinklePhase, variable });
      }
      starsRef.current = arr;
    }

    function buildOffscreen() {
      const rect = canvas.getBoundingClientRect();
      const w = Math.max(1, Math.floor(rect.width));
      const h = Math.max(1, Math.floor(rect.height));
      const off = document.createElement('canvas');
      off.width = w;
      off.height = h;
      const octx = off.getContext('2d')!;
      octx.clearRect(0, 0, w, h);
      const stars = starsRef.current || [];
      const palette = ['#ffffff', '#bfe8ff', '#ffd1d1'];
      for (const s of stars) {
        const color = palette[Math.floor(Math.random() * palette.length)];
        const a = Math.max(0.12, Math.min(1, s.alpha));
        octx.strokeStyle = color;
        octx.globalAlpha = a * 0.95;
        octx.lineWidth = Math.max(1, s.r * 0.7);
        const len = s.r * 4 + 1;
        // draw cardinal lines (N,S,E,W)
        octx.beginPath();
        octx.moveTo(s.x - len, s.y);
        octx.lineTo(s.x + len, s.y);
        octx.moveTo(s.x, s.y - len);
        octx.lineTo(s.x, s.y + len);
        octx.stroke();
        // small center dot
        octx.fillStyle = color;
        octx.globalAlpha = a;
        octx.beginPath();
        octx.arc(s.x, s.y, Math.max(0.6, s.r * 0.6), 0, Math.PI * 2);
        octx.fill();
      }
      // reset
      const octx2 = off.getContext('2d')!;
      octx2.globalAlpha = 1;
      offscreenRef.current = off;
    }

    function triggerSupernova() {
      if (!mounted) return;
      const rect = canvas.getBoundingClientRect();
      const stars = starsRef.current || [];
      const pick = stars[Math.floor(Math.random() * stars.length)];
      const x = pick ? pick.x : Math.random() * rect.width;
      const y = pick ? pick.y : Math.random() * rect.height;
      const now = performance.now();
      supernovasRef.current.push({ x, y, start: now, duration: 2400 + Math.random() * 2600, maxR: 40 + Math.random() * 80 });
    }

    function scheduleSupernova() {
      const delay = 6_000 + Math.random() * 18_000; // 6-24s
      const t = window.setTimeout(() => {
        triggerSupernova();
        scheduleSupernova();
      }, delay);
      timersRef.current.push(t);
    }

    function triggerComet() {
      if (!mounted) return;
      const rect = canvas.getBoundingClientRect();
      const w = rect.width;
      const h = rect.height;
      // start off left/top quadrant, head towards opposite edge
      const startEdge = Math.random() < 0.5 ? 'left' : 'top';
      let x = 0, y = 0, vx = 0, vy = 0;
      if (startEdge === 'left') {
        x = -0.1 * w;
        y = Math.random() * h * 0.6 + h * 0.2;
        vx = w * (0.8 + Math.random() * 0.6) / 1000; // px/ms
        vy = (Math.random() - 0.5) * 0.4 * vx;
      } else {
        x = Math.random() * w * 0.6 + w * 0.2;
        y = -0.1 * h;
        vy = h * (0.8 + Math.random() * 0.6) / 1000;
        vx = (Math.random() - 0.5) * 0.4 * vy;
      }
      const now = performance.now();
      cometsRef.current.push({ x, y, vx, vy, start: now, life: 900 + Math.random() * 900 });
    }

    function scheduleComet() {
      const delay = 8_000 + Math.random() * 24_000; // 8-32s
      const t = window.setTimeout(() => {
        triggerComet();
        scheduleComet();
      }, delay);
      timersRef.current.push(t);
    }

    function draw(now: number) {
      if (!mounted) return;
      const rect = canvas.getBoundingClientRect();
      const w = rect.width;
      const h = rect.height;
      ctx.clearRect(0, 0, w, h);
      // draw static layer
      const off = offscreenRef.current;
      if (off) ctx.drawImage(off, 0, 0, w, h);

      // draw variable stars
      const stars = starsRef.current || [];
      ctx.save();
      ctx.globalCompositeOperation = 'lighter';
      for (const s of stars) {
        if (!s.variable) continue;
        const v = 0.5 + 0.5 * Math.sin(now / 1000 * s.twinkleSpeed + s.twinklePhase);
        const a = Math.max(0.12, Math.min(1, s.alpha * (0.6 + 0.6 * v)));
        const palette = ['#ffffff', '#bfe8ff', '#ffd1d1'];
        const color = palette[Math.floor((s.x + s.y) % palette.length)];
        ctx.strokeStyle = color;
        ctx.fillStyle = color;
        ctx.globalAlpha = a;
        ctx.lineWidth = Math.max(1, s.r * 0.7);
        const len = s.r * 4 + 1;
        ctx.beginPath();
        ctx.moveTo(s.x - len, s.y);
        ctx.lineTo(s.x + len, s.y);
        ctx.moveTo(s.x, s.y - len);
        ctx.lineTo(s.x, s.y + len);
        ctx.stroke();
        ctx.beginPath();
        ctx.arc(s.x, s.y, Math.max(0.6, s.r * 0.6), 0, Math.PI * 2);
        ctx.fill();
        ctx.globalAlpha = 1;
      }

      // draw supernovas
      const sns = supernovasRef.current;
      for (let i = sns.length - 1; i >= 0; i--) {
        const sn = sns[i];
        const t = (now - sn.start) / sn.duration;
        if (t > 1) {
          sns.splice(i, 1);
          continue;
        }
        const ease = Math.sin(Math.min(1, t) * Math.PI * 0.5);
        const r = ease * sn.maxR;
        const grad = ctx.createRadialGradient(sn.x, sn.y, 0, sn.x, sn.y, r * 2);
        grad.addColorStop(0, `rgba(255,240,200,${0.9 * (1 - t)})`);
        grad.addColorStop(0.4, `rgba(255,160,80,${0.6 * (1 - t)})`);
        grad.addColorStop(1, `rgba(255,140,200,${0.02 * (1 - t)})`);
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.arc(sn.x, sn.y, r * 2, 0, Math.PI * 2);
        ctx.fill();
      }

      // draw comets
      const cs = cometsRef.current;
      for (let i = cs.length - 1; i >= 0; i--) {
        const c = cs[i];
        const age = now - c.start;
        if (age > c.life) {
          cs.splice(i, 1);
          continue;
        }
        const px = c.x + c.vx * age;
        const py = c.y + c.vy * age;
        const tailLen = Math.max(40, (1 - age / c.life) * 160);
        // draw trail
        ctx.beginPath();
        const grad = ctx.createLinearGradient(px, py, px - c.vx * tailLen, py - c.vy * tailLen);
        grad.addColorStop(0, 'rgba(220,240,255,0.95)');
        grad.addColorStop(1, 'rgba(120,160,200,0.0)');
        ctx.strokeStyle = grad;
        ctx.lineWidth = 2.5;
        ctx.lineCap = 'round';
        ctx.moveTo(px, py);
        ctx.lineTo(px - c.vx * tailLen, py - c.vy * tailLen);
        ctx.stroke();
        // head
        ctx.fillStyle = 'rgba(255,255,255,0.95)';
        ctx.beginPath();
        ctx.arc(px, py, 2.8, 0, Math.PI * 2);
        ctx.fill();
      }

      ctx.restore();
      rafRef.current = window.requestAnimationFrame(draw);
    }

    function start() {
      const rect = canvas.getBoundingClientRect();
      const area = rect.width * rect.height;
      const base = Math.max(200, Math.min(1500, Math.floor(area / 6000)));
      buildStars(base);
      buildOffscreen();
      if (!prefersReducedMotion()) {
        scheduleSupernova();
        scheduleComet();
      }
      rafRef.current = window.requestAnimationFrame(draw);
    }

    function stop() {
      if (rafRef.current) window.cancelAnimationFrame(rafRef.current);
      for (const t of timersRef.current) window.clearTimeout(t);
      timersRef.current = [];
    }

    resize();
    start();

    const ro = new ResizeObserver(resize);
    ro.observe(canvas);

    return () => {
      mounted = false;
      stop();
      ro.disconnect();
    };
  }, []);

    return (
    <canvas
      ref={canvasRef}
      className={className ?? 'fixed inset-0 w-full h-full pointer-events-none'}
      aria-hidden
    />
  );
}
