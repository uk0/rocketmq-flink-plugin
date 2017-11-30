package org.apache.flink.streaming.api.functions;

import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.awt.geom.Point2D;
import java.util.Vector;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.border.EmptyBorder;

public class Test extends JFrame implements MouseListener, MouseMotionListener {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private JPanel contentPane;
    private Graphics2D g2;
    private static Vector<Point2D> pointInfo;
    /**
     * Launch the application.
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            Test mainFrame = new Test();
            mainFrame.setLocationRelativeTo(null);
            mainFrame.setVisible(true);
        });
    }
    /**
     * Create the frame.
     */
    public Test() {
        addMouseListener(this);
        addMouseMotionListener(this);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setBounds(100, 100, 951, 606);
        contentPane = new JPanel();
        contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
        contentPane.setLayout(new BorderLayout(0, 0));
        setContentPane(contentPane);
        //初始化
        pointInfo=new Vector<>();
    }
    @Override
    public void paint(Graphics g) {
        System.out.println("paint");
        g2 = (Graphics2D) g;
        int size = pointInfo.size();
        Point p1,p2;
        g2.setColor(Color.BLACK);
        g2.setStroke(new BasicStroke(WIDTH));
        for (int i = 0; i < size-1; i++) {
            p1 = (Point) pointInfo.elementAt(i);
            p2 = (Point) pointInfo.elementAt(i + 1);
            g2.drawLine(p1.x, p1.y, p2.x, p2.y);
        }
    }
    public void update(Graphics g) {
        System.out.println("update");
        paint(g);
    }
    @Override
    public void mouseDragged(MouseEvent e) {
        System.out.println("mouseDragged");
        Point cutflag = new Point(e.getX(), e.getY());
        pointInfo.addElement(cutflag);
        repaint();
    }
    @Override
    public void mousePressed(MouseEvent e) {
        Point cutflag = new Point(e.getX(), e.getY());
        pointInfo.addElement(cutflag);
    }
    @Override
    public void mouseReleased(MouseEvent e) {
        Point cutflag = new Point(-1, -1);
        pointInfo.addElement(cutflag);
        pointInfo.clear();
    }
    @Override
    public void mouseMoved(MouseEvent e) {
    }
    @Override
    public void mouseClicked(MouseEvent e) {
    }
    @Override
    public void mouseEntered(MouseEvent e) {
    }

    @Override
    public void mouseExited(MouseEvent e) {
    }
}