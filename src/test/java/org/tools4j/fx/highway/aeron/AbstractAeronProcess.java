/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 fx-highway (tools4j), Marco Terzer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.tools4j.fx.highway.aeron;

import io.aeron.Aeron;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Base class for aeron components started in a separate process.
 */
abstract public class AbstractAeronProcess {

    private final String aeronDirectoryName;

    private Process process;

    public AbstractAeronProcess(final String aeronDirectoryName) {
        this.aeronDirectoryName = Objects.requireNonNull(aeronDirectoryName);
    }

    public String getAeronDirectoryName() {
        return aeronDirectoryName;
    }

    protected void start(final Class<?> mainClass) {
        if (process != null) {
            throw new RuntimeException("media driver already started");
        }
        final ProcessBuilder processBuilder = new ProcessBuilder(command(mainClass));
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        try {
            System.out.println("starting " + getClass().getSimpleName() + " ...");
            System.out.println("\t" + processBuilder.command());
            final Process process = processBuilder.start();
            final long t0 = System.currentTimeMillis();
            while (!process.isAlive() && System.currentTimeMillis() - t0 < 5000) {
                Thread.sleep(100);
            }
            if (process.isAlive()) {
                this.process = process;
                return;
            } else {
                throw new RuntimeException("process exited with exitVal=" + process.exitValue());
            }
        } catch (final Exception e) {
            throw new RuntimeException("starting " + mainClass.getSimpleName() + " failed, e=" + e, e);
        }
    }

    protected List<String> command(final Class<?> mainClass) {
        final String separator = System.getProperty("file.separator");
        final String classpath = System.getProperty("java.class.path");
        final String path = System.getProperty("java.home")
                + separator + "bin" + separator + "java";
        final List<String> command = new ArrayList<>();
        command.add(path);
        command.addAll(jvmArgs());
        command.add("-cp");
        command.add(classpath);
        command.add(mainClass.getName());
        command.addAll(mainArgs());
        return command;
    }

    protected List<String> jvmArgs() {
        final List<String> args = new ArrayList<>();
        args.add("-XX:+UnlockDiagnosticVMOptions");
        args.add("-XX:GuaranteedSafepointInterval=300000");
        args.add("-XX:BiasedLockingStartupDelay=0");
        args.add("-Dagrona.disable.bounds.checks=true");
        return args;
    }

    protected List<String> mainArgs() {
        final List<String> args = new ArrayList<>();
        args.add(getAeronDirectoryName());
        return args;
    }

    public void shutdown() {
        if (process != null) {
            process.destroyForcibly();
            process = null;
        }
    }

    public void waitFor(final long timeout, final TimeUnit unit) {
        if (process != null) {
            try {
                if (process.waitFor(timeout, unit)) {
                    process = null;
                    return;
                }
                throw new RuntimeException("process " + getClass().getSimpleName() + " has not finished after " + timeout + " " + unit);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected static Aeron aeron(final String aeronDirectoryName) {
        final Aeron.Context actx = new Aeron.Context();
        actx.aeronDirectoryName(aeronDirectoryName);
        return Aeron.connect(actx);
    }
}