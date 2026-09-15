import { useEffect, useRef } from 'react';

type Star = {
  x: number;
  y: number;
  z: number;
  r: number;
  angle: number;
  orbitRadius: number;
  orbitSpeed: number;
};

type Explosion = {
  x: number;
  y: number;
  start: number;
  duration: number;
  maxR: number;
};

export default function Kilonova({ className }: { className?: string }) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const rafRef = useRef<number | null>(null);
  const star1Ref = useRef<Star | null>(null);
  const star2Ref = useRef<Star | null>(null);
  const explosionRef = useRef<Explosion | null>(null);
  const animationStartRef = useRef<number>(0);

  useEffect(() => {
    const canvas = canvasRef.current!;
    if (!canvas) return;
    const ctx = canvas.getContext('2d')!;

    let mounted = true;
    const dpr = Math.max(1, window.devicePixelRatio || 1);

    // Animation parameters
    const ORBIT_DURATION = 10000; // 10 seconds to complete orbit and collide
    const EXPLOSION_DURATION = 6000; // 6 seconds for explosion
    const REMNANT_DISPLAY_TIME = 6000; //  seconds to show remnant star (tunable parameter)
    const FADE_IN_DURATION = 1500; // 1.5 second fade in for stars and trails
    const TOTAL_CYCLE = ORBIT_DURATION + EXPLOSION_DURATION + REMNANT_DISPLAY_TIME;
    const TILT_ANGLE = Math.PI / 7.5; // ~24 degrees tilt for perspective view

    function resize() {
      const rect = canvas.getBoundingClientRect();
      canvas.width = Math.max(1, Math.floor(rect.width * dpr));
      canvas.height = Math.max(1, Math.floor(rect.height * dpr));
      canvas.style.width = `${rect.width}px`;
      canvas.style.height = `${rect.height}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      initStars();
    }

    function initStars() {
      const rect = canvas.getBoundingClientRect();
      const centerX = rect.width / 2;
      const centerY = rect.height / 2;
      const baseOrbitRadius = Math.min(rect.width, rect.height) * 0.38;

      // Two stars orbiting in opposite directions
      star1Ref.current = {
        x: centerX,
        y: centerY,
        z: 0,
        r: 14,
        angle: 0,
        orbitRadius: baseOrbitRadius,
        orbitSpeed: (Math.PI * 2) / ORBIT_DURATION,
      };

      star2Ref.current = {
        x: centerX,
        y: centerY,
        z: 0,
        r: 14,
        angle: Math.PI,
        orbitRadius: baseOrbitRadius,
        orbitSpeed: (Math.PI * 2) / ORBIT_DURATION,
      };
    }

    function draw(now: number) {
      if (!mounted) return;

      const rect = canvas.getBoundingClientRect();
      const w = rect.width;
      const h = rect.height;
      const centerX = w / 2;
      const centerY = h / 2;

      ctx.clearRect(0, 0, w, h);

      if (animationStartRef.current === 0) {
        animationStartRef.current = now;
      }

      const elapsed = now - animationStartRef.current;
      const cycleTime = elapsed % TOTAL_CYCLE;

      const star1 = star1Ref.current!;
      const star2 = star2Ref.current!;

      // Phase 1: Orbiting (0 to ORBIT_DURATION)
      if (cycleTime < ORBIT_DURATION) {
        const orbitProgress = cycleTime / ORBIT_DURATION;
        
        // Calculate fade-in alpha for the first FADE_IN_DURATION milliseconds
        const fadeInProgress = Math.min(1, cycleTime / FADE_IN_DURATION);
        const fadeInAlpha = fadeInProgress;
        
        // Stars spiral inward more slowly - use cubic easing for gradual approach
        const spiralEase = orbitProgress * orbitProgress * orbitProgress;
        const currentOrbitRadius = star1.orbitRadius * (1 - spiralEase * 0.96);
        
        // Rotation speed increases dramatically as they get closer
        const speedMultiplier = star1.orbitRadius / Math.max(currentOrbitRadius, star1.orbitRadius * 0.05);
        const cappedMultiplier = Math.min(speedMultiplier, 15); // Cap at 15x original speed
        const effectiveSpeed = star1.orbitSpeed * cappedMultiplier;
        
        // Update star positions in 3D space
        const angle1 = star1.angle + effectiveSpeed * cycleTime;
        const angle2 = star2.angle + effectiveSpeed * cycleTime;

        // Calculate 3D positions
        const x1_3d = Math.cos(angle1) * currentOrbitRadius;
        const z1_3d = Math.sin(angle1) * currentOrbitRadius;
        const x2_3d = Math.cos(angle2) * currentOrbitRadius;
        const z2_3d = Math.sin(angle2) * currentOrbitRadius;

        // Apply perspective projection (tilt the orbital plane)
        star1.z = z1_3d;
        star1.x = centerX + x1_3d;
        star1.y = centerY + z1_3d * Math.sin(TILT_ANGLE);

        star2.z = z2_3d;
        star2.x = centerX + x2_3d;
        star2.y = centerY + z2_3d * Math.sin(TILT_ANGLE);

        // Draw mathematical trails (smooth curves generated from orbit parameters)
        ctx.save();
        ctx.globalAlpha = fadeInAlpha;
        const trailArcLength = Math.PI * 0.5; // Show ~90 degrees of trail
        drawMathematicalTrail(ctx, centerX, centerY, angle1, currentOrbitRadius, trailArcLength, TILT_ANGLE);
        drawMathematicalTrail(ctx, centerX, centerY, angle2, currentOrbitRadius, trailArcLength, TILT_ANGLE);
        ctx.restore();

        // Fade out stars only at the very end
        const fadeOutThreshold = 0.96; // Start fading very late
        const starAlpha = orbitProgress < fadeOutThreshold ? fadeInAlpha : fadeInAlpha * (1 - ((orbitProgress - fadeOutThreshold) / (1 - fadeOutThreshold)));
        
        // Very subtle tidal stretching - only noticeable at the very end
        const stretchFactor = orbitProgress > 0.97 ? 1 + (orbitProgress - 0.97) / 0.03 * 0.6 : 1;
        
        // Blinding flash only at the very last moment (right at collision)
        if (orbitProgress > 0.97) {
          const blindingProgress = Math.min(1, (orbitProgress - 0.97) / 0.03);
          const eased = blindingProgress * blindingProgress * blindingProgress;

          // Intense white blinding flash at merger point
          const flashR = 60 + 180 * eased;
          const grad = ctx.createRadialGradient(centerX, centerY, 0, centerX, centerY, flashR);
          grad.addColorStop(0.0, `rgba(255, 255, 255, ${0.95 * eased})`);
          grad.addColorStop(0.3, `rgba(255, 250, 245, ${0.6 * eased})`);
          grad.addColorStop(0.7, `rgba(255, 240, 230, ${0.2 * eased})`);
          grad.addColorStop(1.0, 'rgba(255, 230, 210, 0)');
          ctx.fillStyle = grad;
          ctx.beginPath();
          ctx.arc(centerX, centerY, flashR, 0, Math.PI * 2);
          ctx.fill();
        }

        // Draw stars with depth-based scaling and ordering
        ctx.save();
        ctx.globalAlpha = starAlpha;
        const stars = [star1, star2].sort((a, b) => a.z - b.z);
        for (const star of stars) {
          const depthScale = 0.7 + 0.3 * ((star.z / star1.orbitRadius + 1) / 2);
          const starSize = star.r * depthScale;
          const dx = centerX - star.x;
          const dy = centerY - star.y;
          const angleToCenter = Math.atan2(dy, dx);
          drawStar(ctx, star.x, star.y, starSize, stretchFactor, angleToCenter);
        }
        ctx.restore();
      }
      // Phase 2: Explosion
      else if (cycleTime < ORBIT_DURATION + EXPLOSION_DURATION) {
        const explosionTime = cycleTime - ORBIT_DURATION;
        
        if (!explosionRef.current || explosionRef.current.start !== animationStartRef.current + ORBIT_DURATION) {
          explosionRef.current = {
            x: centerX,
            y: centerY,
            start: animationStartRef.current + ORBIT_DURATION,
            duration: EXPLOSION_DURATION,
            maxR: Math.min(w, h) * 0.7,
          };
        }

        // Continue blinding flash briefly at start of explosion phase
        const t = explosionTime / EXPLOSION_DURATION;
        if (t < 0.08) {
          const flashFade = 1 - t / 0.08;
          const flashR = 240;
          const grad = ctx.createRadialGradient(centerX, centerY, 0, centerX, centerY, flashR);
          grad.addColorStop(0.0, `rgba(255, 255, 255, ${0.95 * flashFade})`);
          grad.addColorStop(0.3, `rgba(255, 250, 245, ${0.6 * flashFade})`);
          grad.addColorStop(0.7, `rgba(255, 240, 230, ${0.2 * flashFade})`);
          grad.addColorStop(1.0, 'rgba(255, 230, 210, 0)');
          ctx.fillStyle = grad;
          ctx.beginPath();
          ctx.arc(centerX, centerY, flashR, 0, Math.PI * 2);
          ctx.fill();
        }
        
        drawExplosion(ctx, explosionRef.current, explosionTime);
        
        if (t > 0.3) {
          const remnantAlpha = Math.min(1, (t - 0.3) / 0.4);
          const remnantSize = 14 * (0.3 + 0.7 * remnantAlpha);
          
          ctx.save();
          ctx.globalAlpha = remnantAlpha;
          drawStar(ctx, centerX, centerY, remnantSize, 1, 0);
          ctx.restore();
        }
      }
      // Phase 3: Remnant and fade transition
      else if (cycleTime < ORBIT_DURATION + EXPLOSION_DURATION + REMNANT_DISPLAY_TIME) {
        const pauseTime = cycleTime - (ORBIT_DURATION + EXPLOSION_DURATION);
        const pauseProgress = pauseTime / REMNANT_DISPLAY_TIME;
        
        let remnantAlpha = 0;
        if (pauseProgress < 0.4) {
          remnantAlpha = 1;
        } else if (pauseProgress < 0.7) {
          remnantAlpha = 1 - (pauseProgress - 0.4) / 0.3;
        }
        if (remnantAlpha > 0) {
          ctx.save();
          ctx.globalAlpha = remnantAlpha;
          drawStar(ctx, centerX, centerY, 14, 1, 0);
          ctx.restore();
        }
        
        if (pauseProgress > 0.7) {
          const starFadeProgress = Math.min(1, (pauseProgress - 0.7) / 0.3);
          
          ctx.save();
          ctx.globalAlpha = starFadeProgress;
          
          ctx.restore();
        }
      }

      rafRef.current = window.requestAnimationFrame(draw);
    }

    function drawStar(ctx: CanvasRenderingContext2D, x: number, y: number, r: number, stretch = 1, angle = 0) {
      ctx.save();
      ctx.translate(x, y);
      ctx.rotate(angle);
      
      for (let i = 0; i < 3; i++) {
        const glowSize = r * (5 - i * 1.2);
        const alpha = 0.4 / (i + 1);
        const grad = ctx.createRadialGradient(0, 0, 0, 0, 0, glowSize);
        grad.addColorStop(0, `rgba(255, 255, 255, ${alpha})`);
        grad.addColorStop(0.3, `rgba(200, 220, 255, ${alpha * 0.7})`);
        grad.addColorStop(0.6, `rgba(150, 180, 255, ${alpha * 0.3})`);
        grad.addColorStop(1, 'rgba(255, 255, 255, 0)');
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.ellipse(0, 0, glowSize * stretch, glowSize, 0, 0, Math.PI * 2);
        ctx.fill();
      }

      ctx.fillStyle = 'rgb(255, 255, 255)';
      ctx.beginPath();
      ctx.ellipse(0, 0, r * stretch, r, 0, 0, Math.PI * 2);
      ctx.fill();
      
      ctx.restore();
    }

    function drawMathematicalTrail(
      ctx: CanvasRenderingContext2D,
      centerX: number,
      centerY: number,
      currentAngle: number,
      radius: number,
      arcLength: number,
      tilt: number,
    ) {
      ctx.save();
      ctx.globalCompositeOperation = 'lighter';
      ctx.lineCap = 'round';
      ctx.lineJoin = 'round';

      // Generate smooth curve by calculating points along the arc
      const segments = 80; // High resolution
      ctx.beginPath();
      
      for (let i = 0; i <= segments; i++) {
        const progress = i / segments;
        const angle = currentAngle - arcLength * progress;
        
        const x3d = Math.cos(angle) * radius;
        const z3d = Math.sin(angle) * radius;
        
        const x = centerX + x3d;
        const y = centerY + z3d * Math.sin(tilt);
        
        if (i === 0) {
          ctx.moveTo(x, y);
        } else {
          ctx.lineTo(x, y);
        }
      }
      
      // Draw with gradient stroke
      const grad = ctx.createLinearGradient(
        centerX + Math.cos(currentAngle) * radius,
        centerY + Math.sin(currentAngle) * radius * Math.sin(tilt),
        centerX + Math.cos(currentAngle - arcLength) * radius,
        centerY + Math.sin(currentAngle - arcLength) * radius * Math.sin(tilt)
      );
      grad.addColorStop(0, 'rgba(255, 220, 200, 0.35)');
      grad.addColorStop(1, 'rgba(255, 220, 200, 0)');
      
      ctx.strokeStyle = grad;
      ctx.lineWidth = 2.5;
      ctx.stroke();
      
      ctx.restore();
    }

    function drawExplosion(ctx: CanvasRenderingContext2D, explosion: Explosion, elapsed: number) {
      const t = elapsed / explosion.duration;
      if (t > 1) return;

      const x = explosion.x;
      const y = explosion.y;

      if (t < 0.15) {
        const flashAlpha = (1 - t / 0.15) * 0.6;
        const flashSize = 40 + t / 0.15 * 30;
        ctx.fillStyle = `rgba(255, 220, 180, ${flashAlpha})`;
        ctx.beginPath();
        ctx.arc(x, y, flashSize, 0, Math.PI * 2);
        ctx.fill();
      }

      const shockwaves = [0.08, 0.18, 0.28];
      for (const shockStart of shockwaves) {
        if (t > shockStart && t < shockStart + 0.35) {
          const shockT = (t - shockStart) / 0.35;
          const shockR = shockT * explosion.maxR * 1.15;
          const shockAlpha = (1 - shockT) * 0.25;
          
          ctx.strokeStyle = `rgba(200, 100, 80, ${shockAlpha})`;
          ctx.lineWidth = 2 * (1 - shockT * 0.6);
          ctx.beginPath();
          ctx.arc(x, y, shockR, 0, Math.PI * 2);
          ctx.stroke();
        }
      }

      const rings = [
        { color: [200, 100, 80], stop: 0.15, alpha: 0.85 },
        { color: [220, 80, 60], stop: 0.3, alpha: 0.8 },
        { color: [200, 60, 80], stop: 0.45, alpha: 0.75 },
        { color: [180, 50, 100], stop: 0.58, alpha: 0.65 },
        { color: [160, 40, 120], stop: 0.7, alpha: 0.55 },
        { color: [140, 30, 100], stop: 0.82, alpha: 0.4 },
        { color: [120, 20, 80], stop: 0.92, alpha: 0.25 },
        { color: [100, 10, 60], stop: 1.0, alpha: 0.1 },
      ];

      for (const ring of rings) {
        const ringProgress = Math.min(1, Math.max(0, (t - (1 - ring.stop) * 0.15) / ring.stop));
        const r = ringProgress * explosion.maxR * 1.2;
        const fadeOut = Math.max(0, 1 - t);
        const alpha = ring.alpha * fadeOut * (1 - ringProgress * 0.15);

        const grad = ctx.createRadialGradient(x, y, 0, x, y, r * 1.3);
        grad.addColorStop(0, `rgba(${ring.color[0]}, ${ring.color[1]}, ${ring.color[2]}, ${alpha})`);
        grad.addColorStop(0.35, `rgba(${ring.color[0]}, ${ring.color[1]}, ${ring.color[2]}, ${alpha * 0.6})`);
        grad.addColorStop(0.7, `rgba(${ring.color[0]}, ${ring.color[1]}, ${ring.color[2]}, ${alpha * 0.2})`);
        grad.addColorStop(1, `rgba(${ring.color[0]}, ${ring.color[1]}, ${ring.color[2]}, 0)`);

        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.arc(x, y, r * 1.3, 0, Math.PI * 2);
        ctx.fill();
      }

      if (t < 0.6) {
        const coreT = t / 0.6;
        const coreR = coreT * 40;
        const coreAlpha = (1 - coreT) * 0.7;
        const grad = ctx.createRadialGradient(x, y, 0, x, y, coreR);
        grad.addColorStop(0, `rgba(220, 120, 100, ${coreAlpha})`);
        grad.addColorStop(0.4, `rgba(200, 80, 100, ${coreAlpha * 0.5})`);
        grad.addColorStop(1, `rgba(180, 60, 100, 0)`);
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.arc(x, y, coreR, 0, Math.PI * 2);
        ctx.fill();
      }

      if (t < 0.7) {
        const particleCount = 12;
        const particleT = t / 0.7;
        ctx.save();
        ctx.globalCompositeOperation = 'lighter';
        for (let i = 0; i < particleCount; i++) {
          const angle = (i / particleCount) * Math.PI * 2;
          const dist = particleT * explosion.maxR * 0.6;
          const px = x + Math.cos(angle) * dist;
          const py = y + Math.sin(angle) * dist;
          const particleAlpha = (1 - particleT) * 0.4;
          
          const grad = ctx.createRadialGradient(px, py, 0, px, py, 6);
          grad.addColorStop(0, `rgba(200, 100, 120, ${particleAlpha})`);
          grad.addColorStop(1, `rgba(150, 50, 80, 0)`);
          ctx.fillStyle = grad;
          ctx.beginPath();
          ctx.arc(px, py, 6, 0, Math.PI * 2);
          ctx.fill();
          
          ctx.strokeStyle = `rgba(180, 80, 100, ${particleAlpha * 0.4})`;
          ctx.lineWidth = 1.5;
          ctx.beginPath();
          ctx.moveTo(x, y);
          ctx.lineTo(px, py);
          ctx.stroke();
        }
        ctx.restore();
      }
    }

    function start() {
      initStars();
      animationStartRef.current = 0;
      rafRef.current = window.requestAnimationFrame(draw);
    }

    function stop() {
      if (rafRef.current) window.cancelAnimationFrame(rafRef.current);
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
